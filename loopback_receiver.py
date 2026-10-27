#!/usr/bin/env python3
"""
UDP-Audio-Empfänger für den WASAPI-Loopback-Streamer.

Empfängt 16-Bit-PCM-Pakete mit 16-Byte-Header und spielt sie auf dem
Standard-Ausgabegerät ab und/oder schreibt sie als WAV-Datei.

Abhängigkeiten:  pip install numpy sounddevice
"""

import argparse
import queue
import socket
import struct
import sys
import threading
import time
import wave
from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import sounddevice as sd

# --------------------------------------------------------------- Protokoll
# Muss exakt zum Sender passen (dort #pragma pack(push,1)):
#   uint32 magic; uint16 sequence; uint16 payloadBytes;
#   uint32 sampleRate; uint16 channels; uint16 bitsPerSample;
#
# '<' = Little-Endian, weil der Sender den Struct per memcpy in nativer
#       x86-Byteordnung rausschreibt.
# I H H I H H  ->  4 + 2 + 2 + 4 + 2 + 2 = 16 Bytes
HEADER_FORMAT = "<IHHIHH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
assert HEADER_SIZE == 16, HEADER_SIZE

MAGIC = 0x4F4D554C          # 'LUMO'
MAX_DATAGRAM = 65535

# ---------------------------------------------------------------- Defaults
DEFAULT_PORT = 5000
DEFAULT_JITTER_MS = 140.0   # Ziel-Füllstand des Puffers
DEFAULT_PREBUFFER_MS = 70.0 # Mindestfüllung vor Wiedergabestart
MAX_PAD_PACKETS = 40        # Obergrenze für Stille-Einschub bei Paketverlust


@dataclass
class PacketHeader:
    magic: int
    sequence: int
    payload_bytes: int
    sample_rate: int
    channels: int
    bits_per_sample: int


def parse_packet(data: bytes) -> Optional[PacketHeader]:
    """Header lesen. Gibt None zurück, wenn das Paket nicht passt."""
    if len(data) < HEADER_SIZE:
        return None

    # unpack_from akzeptiert Puffer, die laenger als das Format sind.
    magic, seq, payload_bytes, sample_rate, channels, bits = \
        struct.unpack_from(HEADER_FORMAT, data, 0)

    if magic != MAGIC:
        return None
    if bits != 16:
        return None
    if channels < 1 or channels > 8:
        return None
    if sample_rate < 8000 or sample_rate > 384000:
        return None
    if payload_bytes == 0 or payload_bytes % (2 * channels) != 0:
        return None
    if len(data) < HEADER_SIZE + payload_bytes:
        return None

    return PacketHeader(magic, seq, payload_bytes, sample_rate, channels, bits)


class AudioReceiver:
    def __init__(self, bind_addr, port, play_audio=True, record_file=None,
                 device=None, gain=1.0, jitter_ms=DEFAULT_JITTER_MS,
                 prebuffer_ms=DEFAULT_PREBUFFER_MS, quiet=False):
        self.bind_addr = bind_addr
        self.port = port
        self.play_audio = play_audio
        self.record_file = record_file
        self.device = device
        self.gain = gain
        self.jitter_ms = jitter_ms
        self.prebuffer_ms = prebuffer_ms
        self.quiet = quiet

        self.sock: Optional[socket.socket] = None
        self.stream: Optional[sd.OutputStream] = None
        self.running = False
        self.finished = False

        self.sample_rate = 0
        self.channels = 0
        self.frames_per_packet = 0

        self.audio_q: "queue.Queue[np.ndarray]" = queue.Queue(maxsize=8192)
        self.buf_lock = threading.Lock()
        self.buffered_frames = 0

        self.rec_lock = threading.Lock()
        self.recording: List[np.ndarray] = []
        self.recorded_frames = 0

        self.rx_thread: Optional[threading.Thread] = None
        self.play_thread: Optional[threading.Thread] = None

        # Statistik
        self.packets_ok = 0
        self.packets_bad = 0
        self.packets_lost = 0
        self.packets_ooo = 0
        self.bytes_rx = 0
        self.last_seq: Optional[int] = None
        self.started_at = 0.0
        self.underruns = 0

    # ------------------------------------------------------------ Hilfen
    def log(self, msg: str):
        if not self.quiet:
            print(msg, flush=True)

    def _push(self, chunk: np.ndarray):
        """Chunk in die Warteschlange legen (Drop-Tail bei Überlauf)."""
        try:
            self.audio_q.put_nowait(chunk)
            with self.buf_lock:
                self.buffered_frames += len(chunk)
        except queue.Full:
            self.underruns += 1

    def _pop(self, timeout: float) -> Optional[np.ndarray]:
        try:
            chunk = self.audio_q.get(timeout=timeout)
        except queue.Empty:
            return None
        with self.buf_lock:
            self.buffered_frames -= len(chunk)
        return chunk

    def _buffered(self) -> int:
        with self.buf_lock:
            return self.buffered_frames

    def _record(self, chunk: np.ndarray):
        if not self.record_file:
            return
        with self.rec_lock:
            self.recording.append(chunk.copy())
            self.recorded_frames += len(chunk)

    # ------------------------------------------------------- Audio-Ausgang
    def _open_output(self) -> bool:
        try:
            dev_index = self.device
            if dev_index is None:
                dev_index = sd.default.device[1]
            if dev_index is None or dev_index < 0:
                print("[ERROR] Kein Ausgabegerät gefunden.", file=sys.stderr)
                return False

            info = sd.query_devices(dev_index)
            self.log(f"[INFO] Ausgabegerät #{dev_index}: {info['name']}")
            self.log(f"[INFO] Stream: {self.sample_rate} Hz, "
                     f"{self.channels} Kanal/Kanäle, 16 Bit")

            max_out = int(info.get("max_output_channels", 0))
            if max_out and self.channels > max_out:
                print(f"[WARN] Gerät kann nur {max_out} Kanäle, "
                      f"Stream liefert {self.channels}.", file=sys.stderr)

            try:
                self.stream = sd.OutputStream(
                    samplerate=self.sample_rate,
                    channels=self.channels,
                    dtype="float32",
                    device=dev_index,
                    blocksize=0,
                    latency="low",
                )
            except Exception:
                # Manche Backends mögen latency="low" nicht
                self.stream = sd.OutputStream(
                    samplerate=self.sample_rate,
                    channels=self.channels,
                    dtype="float32",
                    device=dev_index,
                    blocksize=0,
                )

            self.stream.start()
            return True
        except Exception as e:
            print(f"[ERROR] Audio-Ausgang fehlgeschlagen: {e}", file=sys.stderr)
            return False

    # --------------------------------------------------------- Empfangen
    def receive_loop(self):
        self.log(f"[INFO] Höre auf UDP {self.bind_addr}:{self.port} ...")
        bad_logged = 0

        while self.running:
            try:
                data, addr = self.sock.recvfrom(MAX_DATAGRAM)
            except socket.timeout:
                continue
            except OSError:
                break

            if not self.running:
                break

            hdr = parse_packet(data)
            if hdr is None:
                self.packets_bad += 1
                if bad_logged < 5:
                    head = data[:HEADER_SIZE].hex(" ")
                    print(f"[WARN] Verworfenes Paket von {addr[0]}:{addr[1]} "
                          f"({len(data)} B) Header: {head}", file=sys.stderr)
                    bad_logged += 1
                continue

            # Erstes gültiges Paket bestimmt das Format
            if self.sample_rate == 0:
                self.sample_rate = hdr.sample_rate
                self.channels = hdr.channels
                self.started_at = time.time()
                self.log(f"[INFO] Stream-Format erkannt: {self.sample_rate} Hz, "
                         f"{self.channels} Kanäle, {hdr.bits_per_sample} Bit")

                if self.play_audio and not self._open_output():
                    self.running = False
                    break

            frames = hdr.payload_bytes // (2 * hdr.channels)
            self.frames_per_packet = frames

            self._check_sequence(hdr.sequence)

            payload = data[HEADER_SIZE:HEADER_SIZE + hdr.payload_bytes]
            samples = np.frombuffer(payload, dtype="<i2").astype(np.float32)
            if self.gain != 1.0:
                samples *= self.gain
            samples /= 32768.0
            np.clip(samples, -1.0, 1.0, out=samples)

            chunk = samples.reshape(-1, self.channels)
            self._push(chunk)

            self.packets_ok += 1
            self.bytes_rx += len(data)

            if not self.quiet and self.packets_ok % 4000 == 0:
                self.print_stats(prefix="[STATS]")

        self.running = False
        self.finished = True

    def _check_sequence(self, seq: int):
        if self.last_seq is None:
            self.last_seq = seq
            return

        expected = (self.last_seq + 1) & 0xFFFF
        if seq == expected:
            self.last_seq = seq
            return

        delta = (seq - expected) & 0xFFFF
        if delta == 0 or delta > 32000:
            self.packets_ooo += 1
            return

        self.packets_lost += delta
        if self.frames_per_packet:
            pad = min(delta, MAX_PAD_PACKETS) * self.frames_per_packet
            self._push(np.zeros((pad, self.channels), dtype=np.float32))
        self.last_seq = seq

    # -------------------------------------------------------- Wiedergabe
    def playback_loop(self):
        prebuffer = int(self.sample_rate * self.prebuffer_ms / 1000.0)
        deadline = time.time() + 3.0

        while (self.running and self._buffered() < prebuffer
               and time.time() < deadline):
            time.sleep(0.005)

        if not self.running:
            return

        self.log(f"[INFO] Wiedergabe startet "
                 f"(Puffer: {self._buffered()} Frames)")
        block = max(1, int(self.sample_rate * 0.02))   # 20 ms Stille-Block

        while self.running:
            chunk = self._pop(timeout=0.05)

            if chunk is None:
                # Unterlauf: Stille einschieben, damit die Zeitlinie weiterläuft
                if self.play_audio and self.stream is not None:
                    self.underruns += 1
                    try:
                        self.stream.write(
                            np.zeros((block, self.channels), dtype=np.float32))
                    except Exception:
                        break
                continue

            self._record(chunk)

            if self.play_audio and self.stream is not None:
                try:
                    self.stream.write(chunk)
                except Exception as e:
                    print(f"[ERROR] Schreibfehler am Ausgang: {e}", file=sys.stderr)
                    break

        self.log("[INFO] Wiedergabe-Thread beendet.")

    # ----------------------------------------------------------- Steuerung
    def start(self):
        self.running = True
        self.finished = False

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
        except OSError:
            pass
        self.sock.settimeout(0.5)
        self.sock.bind((self.bind_addr, self.port))

        self.rx_thread = threading.Thread(target=self.receive_loop, daemon=True)
        self.play_thread = threading.Thread(target=self.playback_loop, daemon=True)
        self.rx_thread.start()
        self.play_thread.start()

    def stop(self):
        self.running = False

        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

        for t in (self.rx_thread, self.play_thread):
            if t and t.is_alive():
                t.join(timeout=2.0)

        if self.stream:
            try:
                self.stream.stop()
                self.stream.close()
            except Exception:
                pass
            self.stream = None

    def wait(self):
        if self.rx_thread:
            self.rx_thread.join()

    # -------------------------------------------------------------- Stats
    def print_stats(self, prefix=""):
        elapsed = time.time() - self.started_at if self.started_at else 0.0
        kbps = (self.bytes_rx * 8 / elapsed / 1000.0) if elapsed > 0 else 0.0
        line = (f"{prefix} Pakete: {self.packets_ok:,} | "
                f"verloren: {self.packets_lost:,} | "
                f"ungültig: {self.packets_bad:,} | "
                f"Out-of-order: {self.packets_ooo:,} | "
                f"Unterläufe: {self.underruns:,} | "
                f"Puffer: {self._buffered()} Frames | "
                f"{kbps:.1f} kb/s")
        print(line, flush=True)

    # ---------------------------------------------------------- Aufnahme
    def save_recording(self, path: str):
        with self.rec_lock:
            chunks = self.recording
            total = self.recorded_frames

        if not chunks or total == 0:
            print("[WARN] Keine Audiodaten aufgenommen.", file=sys.stderr)
            return

        print(f"[INFO] Schreibe {total} Frames nach {path} ...")
        data = np.concatenate(chunks, axis=0)
        pcm = np.clip(data * 32767.0, -32768, 32767).astype("<i2")

        with wave.open(path, "wb") as wf:
            wf.setnchannels(self.channels or 1)
            wf.setsampwidth(2)
            wf.setframerate(self.sample_rate or 48000)
            wf.writeframes(pcm.tobytes())

        dur = total / float(self.sample_rate or 48000)
        print(f"[INFO] Gespeichert: {path} ({dur:.1f} s)")


def list_devices():
    print(sd.query_devices())
    try:
        hostapis = sd.query_hostapis()
        print("\nHost-APIs:")
        for i, api in enumerate(hostapis):
            print(f"  [{i}] {api['name']}  default_output={api.get('default_output_device')}")
    except Exception:
        pass


def main():
    p = argparse.ArgumentParser(
        description="UDP-Audio-Empfänger für den WASAPI-Loopback-Streamer")
    p.add_argument("--host", default="0.0.0.0", help="Bind-Adresse (default 0.0.0.0)")
    p.add_argument("--port", type=int, default=DEFAULT_PORT, help="UDP-Port")
    p.add_argument("--device", type=int, default=None, help="Ausgabegerät-Index")
    p.add_argument("--list-devices", action="store_true", help="Geräte auflisten")
    p.add_argument("--no-play", action="store_true", help="Keine Wiedergabe")
    p.add_argument("--record", metavar="FILE", help="WAV-Datei schreiben")
    p.add_argument("--gain", type=float, default=1.0, help="Lautstärkefaktor")
    p.add_argument("--jitter-ms", type=float, default=DEFAULT_JITTER_MS,
                   help="Ziel-Füllstand des Jitter-Puffers")
    p.add_argument("--prebuffer-ms", type=float, default=DEFAULT_PREBUFFER_MS,
                   help="Mindestfüllung vor Wiedergabestart")
    p.add_argument("-q", "--quiet", action="store_true", help="Wenig Ausgabe")
    args = p.parse_args()

    if args.list_devices:
        list_devices()
        return 0

    rx = AudioReceiver(
        bind_addr=args.host,
        port=args.port,
        play_audio=not args.no_play,
        record_file=args.record,
        device=args.device,
        gain=args.gain,
        jitter_ms=args.jitter_ms,
        prebuffer_ms=args.prebuffer_ms,
        quiet=args.quiet,
    )

    try:
        rx.start()
        rx.wait()
    except KeyboardInterrupt:
        print("\n[INFO] Abgebrochen.")
    finally:
        rx.stop()
        if args.record:
            rx.save_recording(args.record)
        print("\n=== Zusammenfassung ===")
        rx.print_stats(prefix="[FINAL]")

    return 0


if __name__ == "__main__":
    sys.exit(main())
