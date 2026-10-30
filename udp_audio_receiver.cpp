// ===========================================================================
//  udp_audio_receiver.cpp   (C++17, Linux, libpulse)
//
//  UDP-Audio-Empfaenger fuer den WASAPI-Loopback-Streamer.
//  Empfaengt 16-Bit-PCM-Pakete mit 16-Byte-Header und spielt sie ueber
//  PulseAudio (Standard-Sink) ab. WAV-Aufzeichnung wie im Python-Original
//  bewusst weggelassen.
//
//  Protokoll (Little-Endian, identisch zum Sender):
//    uint32 magic ('LUMO'), uint16 sequence, uint16 payloadBytes,
//    uint32 sampleRate, uint16 channels, uint16 bitsPerSample
//
//  Build:
//    g++ -O2 -march=native -std=c++17 udp_audio_receiver.cpp \
//        -o udp_rx -lpthread -lpulse -lpulse-simple
// ===========================================================================

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <pulse/error.h>
#include <pulse/simple.h>

namespace {

// -------------------------------------------------------------- Protokoll --
constexpr uint32_t kMagic = 0x4F4D554Cu; // 'LUMO' little-endian
constexpr size_t kHeaderSize = 16;
constexpr int kDefaultPort = 5000;

constexpr double kDefaultPrebufferMs = 70.0;
constexpr unsigned kMaxPadPackets = 40; // Stille-Einschub bei Verlust

constexpr int kRecvBufBytes = 1 << 21;                  // 2 MiB Kernel-RCVBUF
constexpr size_t kRingCapacityFrames = size_t(1) << 19; // ~10.9 s @ 48 kHz
constexpr size_t kScratchFloats = 32768;                // max. Payload/2

// ------------------------------------------------------------------ Zeit ---
inline double monoNow() {
  return std::chrono::duration<double>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// ------------------------------------------------- lock-freier SPSC-Ring --
// Ein Producer (Rx-Thread), ein Consumer (Playback-Thread). Indizes laufen
// monoton und werden per acquire/release gepaart -> kein Mutex, kein ALOK.
class RingBuffer {
public:
  void init(size_t capacityFrames, size_t channels) {
    cap_ = 1;
    while (cap_ < capacityFrames)
      cap_ <<= 1; // Zweierpotenz: Maske statt Modulo
    mask_ = cap_ - 1;
    ch_ = channels;
    buf_.assign(cap_ * ch_, 0.0f);
    rIdx_.store(0, std::memory_order_relaxed);
    wIdx_.store(0, std::memory_order_relaxed);
  }

  size_t channels() const { return ch_; }

  void push(const float *src, size_t frames) {
    const size_t r = rIdx_.load(std::memory_order_acquire);
    const size_t w = wIdx_.load(std::memory_order_relaxed);
    const size_t avail = cap_ - (w - r);
    if (avail == 0)
      return; // voll -> Frame verwerfen
    const size_t n = frames < avail ? frames : avail;
    const size_t first = std::min(n, cap_ - (w & mask_));
    std::memcpy(&buf_[(w & mask_) * ch_], src, first * ch_ * sizeof(float));
    if (n > first)
      std::memcpy(&buf_[0], src + first * ch_, (n - first) * ch_ * sizeof(float));
    wIdx_.store(w + n, std::memory_order_release);
  }

  size_t buffered() const {
    return wIdx_.load(std::memory_order_acquire) - rIdx_.load(std::memory_order_acquire);
  }

  size_t pop(float *dst, size_t frames) {
    const size_t r = rIdx_.load(std::memory_order_relaxed);
    const size_t w = wIdx_.load(std::memory_order_acquire);
    const size_t avail = w - r;
    const size_t n = frames < avail ? frames : avail;
    const size_t first = std::min(n, cap_ - (r & mask_));
    std::memcpy(dst, &buf_[(r & mask_) * ch_], first * ch_ * sizeof(float));
    if (n > first)
      std::memcpy(dst + first * ch_, &buf_[0], (n - first) * ch_ * sizeof(float));
    rIdx_.store(r + n, std::memory_order_release);
    return n;
  }

private:
  std::vector<float> buf_;
  size_t cap_ = 0, mask_ = 0, ch_ = 0;
  std::atomic<size_t> rIdx_{0}, wIdx_{0};
};

// ------------------------------------------------------------- Parameter ---
struct Params {
  std::string bindAddr = "0.0.0.0";
  int port = kDefaultPort;
  bool play = true;
  std::string sink; // leer = Standard-Sink
  float gain = 1.0f;
  double prebufferMs = kDefaultPrebufferMs;
  bool quiet = false;
};

std::atomic<bool> gRunning{true};

// -------------------------------------------------------------- Receiver ---
class Receiver {
public:
  bool start(const Params &p);
  void run();                      // Empfangs-Thread
  void playbackLoop(pa_simple *s); // Wiedergabe-Thread

  void printStats(const char *prefix) const;

  // Format-Handshake: Rx-Thread setzt, Haupt-/Playback-Thread liest.
  std::atomic<int> sampleRate_{0};
  std::atomic<int> channels_{0};

  size_t ringBuffered() const { return ring_.buffered(); }
  int fd() const { return sock_; }

private:
  void checkSequence_(uint16_t seq);

  Params p_;
  int sock_ = -1;
  std::vector<uint8_t> dg_; // Empfangs-Datagramm
  RingBuffer ring_;
  std::vector<float> scratch_; // konvertierte Samples eines Pakets
  std::vector<float> zeros_;   // Stille-Puffer fuer Paketverluste

  std::atomic<uint64_t> packetsOk_{0}, packetsBad_{0}, packetsLost_{0};
  std::atomic<uint64_t> packetsOoo_{0}, underruns_{0}, bytesRx_{0};

  uint16_t framesPerPacket_ = 0;
  double startedAt_ = 0.0;
  uint16_t lastSeq_ = 0;
  bool seqInit_ = false;
};

bool Receiver::start(const Params &p) {
  p_ = p;
  dg_.resize(65536);
  scratch_.resize(kScratchFloats);
  // Worst Case: 40 Pakete * max. Frames/Paket * Kanäle
  zeros_.assign(size_t(kMaxPadPackets) * kScratchFloats, 0.0f);

  sock_ = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (sock_ < 0) {
    perror("socket");
    return false;
  }

  int one = 1;
  setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  setsockopt(sock_, SOL_SOCKET, SO_RCVBUF,
             &kRecvBufBytes, sizeof(kRecvBufBytes));
  timeval tv{0, 500000}; // 500 ms -> kontrollierter Thread-Exit
  setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(p_.port));
  if (inet_pton(AF_INET, p_.bindAddr.c_str(), &addr.sin_addr) != 1) {
    std::fprintf(stderr, "[ERROR] Ungueltige Bind-Adresse: %s\n",
                 p_.bindAddr.c_str());
    return false;
  }
  if (bind(sock_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    perror("bind");
    return false;
  }
  return true;
}

void Receiver::checkSequence_(uint16_t seq) {
  if (!seqInit_) {
    lastSeq_ = seq;
    seqInit_ = true;
    return;
  }

  const uint16_t expected = static_cast<uint16_t>(lastSeq_ + 1);
  if (seq == expected) {
    lastSeq_ = seq;
    return;
  }

  const uint32_t delta = static_cast<uint32_t>(seq - expected);
  if (delta > 32000) {
    packetsOoo_++;
    return;
  } // alt / dupliziert

  packetsLost_ += delta;
  if (framesPerPacket_ > 0) {
    // Stille einschieben (max. 40 Pakete), damit die Zeitlinie
    // weiterlaeuft -> gleiche Logik wie im Python-Original.
    const size_t padFrames =
        std::min<uint32_t>(delta, kMaxPadPackets) * framesPerPacket_;
    ring_.push(zeros_.data(), padFrames);
  }
  lastSeq_ = seq;
}

void Receiver::run() {
  if (!p_.quiet)
    std::printf("[INFO] Hoere auf UDP %s:%d ...\n",
                p_.bindAddr.c_str(), p_.port);

  int badLogged = 0;
  const float inv = 1.0f / 32768.0f;
  const float gain = p_.gain;

  while (gRunning.load(std::memory_order_relaxed)) {
    sockaddr_in from{};
    socklen_t flen = sizeof(from);
    const ssize_t n = ::recvfrom(sock_, dg_.data(), dg_.size(), 0,
                                 reinterpret_cast<sockaddr *>(&from), &flen);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
        continue;
      break;
    }
    if (!gRunning.load(std::memory_order_relaxed))
      break;

    // ---------- Header (explizites Little-Endian) ----------
    if (n < static_cast<ssize_t>(kHeaderSize)) {
      packetsBad_++;
      continue;
    }

    const uint8_t *h = dg_.data();
    const uint32_t magic = uint32_t(h[0]) | (uint32_t(h[1]) << 8) | (uint32_t(h[2]) << 16) | (uint32_t(h[3]) << 24);
    const uint16_t seq = uint16_t(h[4]) | (uint16_t(h[5]) << 8);
    const uint16_t pb = uint16_t(h[6]) | (uint16_t(h[7]) << 8);
    const uint32_t sampleRate = uint32_t(h[8]) | (uint32_t(h[9]) << 8) | (uint32_t(h[10]) << 16) | (uint32_t(h[11]) << 24);
    const uint16_t channels = uint16_t(h[12]) | (uint16_t(h[13]) << 8);
    const uint16_t bits = uint16_t(h[14]) | (uint16_t(h[15]) << 8);

    if (magic != kMagic || bits != 16 ||
        channels < 1 || channels > 8 ||
        sampleRate < 8000 || sampleRate > 384000 ||
        pb == 0 || (pb % (2u * channels)) != 0 ||
        size_t(n) < kHeaderSize + pb) {
      packetsBad_++;
      if (badLogged < 5) {
        char head[kHeaderSize * 3 + 1] = {0};
        for (size_t i = 0; i < kHeaderSize; ++i)
          std::snprintf(head + i * 3, 4, "%02X ", h[i]);
        std::fprintf(stderr,
                     "[WARN] Verworfenes Paket von %s:%u (%zd B) Header: %s\n",
                     inet_ntoa(from.sin_addr), ntohs(from.sin_port),
                     n, head);
        badLogged++;
      }
      continue;
    }

    // ---------- Erstes gueltiges Paket bestimmt das Format ----------
    if (sampleRate_.load(std::memory_order_acquire) == 0) {
      channels_.store(int(channels), std::memory_order_release);
      sampleRate_.store(int(sampleRate), std::memory_order_release);
      startedAt_ = monoNow();
      ring_.init(kRingCapacityFrames, channels);
      if (!p_.quiet)
        std::printf("[INFO] Stream-Format erkannt: %u Hz, %u Kanal/Kanaele, 16 Bit\n",
                    sampleRate, channels);
    }

    const size_t frames = size_t(pb) / (2u * channels);
    framesPerPacket_ = static_cast<uint16_t>(frames);
    checkSequence_(seq);

    // ---------- i16 -> float32, Gain + Clip in einem Durchlauf ----------
    const size_t samples = frames * channels;
    const int16_t *pcm =
        reinterpret_cast<const int16_t *>(dg_.data() + kHeaderSize);
    for (size_t i = 0; i < samples; ++i) {
      float v = float(pcm[i]) * (inv * gain);
      if (v > 1.0f)
        v = 1.0f;
      else if (v < -1.0f)
        v = -1.0f;
      scratch_[i] = v;
    }
    ring_.push(scratch_.data(), frames);

    bytesRx_ += uint64_t(n);
    const uint64_t ok = ++packetsOk_;
    if (!p_.quiet && ok % 4000 == 0)
      printStats("[STATS]");
  }

  gRunning.store(false, std::memory_order_release);
}

void Receiver::playbackLoop(pa_simple *s) {
  const int sr = sampleRate_.load(std::memory_order_acquire);
  const size_t ch = size_t(channels_.load(std::memory_order_acquire));

  // ---------- Prebuffer: Mindestfuellung abwarten (max. 3 s) ----------
  const size_t pre = size_t(double(sr) * p_.prebufferMs / 1000.0);
  const double deadline = monoNow() + 3.0;
  while (gRunning.load() && ring_.buffered() < pre && monoNow() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  if (!gRunning.load())
    return;

  if (!p_.quiet)
    std::printf("[INFO] Wiedergabe startet (Puffer: %zu Frames)\n",
                ring_.buffered());

  const size_t blockFrames = std::max<size_t>(1, size_t(sr) * 0.02); // 20 ms
  std::vector<float> block(blockFrames * ch);
  int err = 0;

  while (gRunning.load(std::memory_order_relaxed)) {
    if (ring_.buffered() == 0) {
      // Unterlauf: Stille einschieben, damit die Zeitlinie laeuft.
      underruns_++;
      std::fill(block.begin(), block.end(), 0.0f);
      if (pa_simple_write(s, block.data(),
                          block.size() * sizeof(float), &err) < 0) {
        std::fprintf(stderr, "[ERROR] pa_simple_write: %s\n",
                     pa_strerror(err));
        break;
      }
      continue;
    }

    const size_t n = std::min(ring_.buffered(), blockFrames);
    const size_t got = ring_.pop(block.data(), n);
    if (pa_simple_write(s, block.data(), got * ch * sizeof(float), &err) < 0) {
      std::fprintf(stderr, "[ERROR] pa_simple_write: %s\n", pa_strerror(err));
      break;
    }
  }
}

void Receiver::printStats(const char *prefix) const {
  const double elapsed = startedAt_ ? monoNow() - startedAt_ : 0.0;
  const double kbps = elapsed > 0.0
                          ? double(bytesRx_.load()) * 8.0 / elapsed / 1000.0
                          : 0.0;
  std::printf("%s Pakete: %" PRIu64 " | verloren: %" PRIu64
              " | ungueltig: %" PRIu64 " | Out-of-order: %" PRIu64
              " | Unterlaeufe: %" PRIu64 " | Puffer: %zu Frames | %.1f kb/s\n",
              prefix, packetsOk_.load(), packetsLost_.load(),
              packetsBad_.load(), packetsOoo_.load(),
              underruns_.load(), ring_.buffered(), kbps);
  std::fflush(stdout);
}

// ------------------------------------------------------------------- CLI ---
void parseArgs(int argc, char **argv, Params &p) {
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&](const char *opt) -> const char * {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "Fehlender Wert fuer %s\n", opt);
        std::exit(2);
      }
      return argv[++i];
    };
    if (a == "--help" || a == "-h") {
      std::printf(
          "Usage: udp_rx [--host ADDR] [--port N] [--sink NAME]\n"
          "              [--no-play] [--gain F] [--prebuffer-ms F] [-q]\n"
          "\n"
          "  --sink       PulseAudio-Sink (leer = Standard); Liste:\n"
          "               pactl list short sinks\n");
      std::exit(0);
    } else if (a == "--host")
      p.bindAddr = next("--host");
    else if (a == "--port")
      p.port = std::atoi(next("--port"));
    else if (a == "--sink")
      p.sink = next("--sink");
    else if (a == "--no-play")
      p.play = false;
    else if (a == "--gain")
      p.gain = float(std::atof(next("--gain")));
    else if (a == "--prebuffer-ms")
      p.prebufferMs = std::atof(next("--prebuffer-ms"));
    else if (a == "-q" || a == "--quiet")
      p.quiet = true;
    else {
      std::fprintf(stderr, "Unbekannte Option: %s\n", a.c_str());
      std::exit(2);
    }
  }
}

} // namespace

int main(int argc, char **argv) {
  Params p;
  parseArgs(argc, argv, p);

  std::signal(SIGINT, [](int) { gRunning.store(false); });
  std::signal(SIGTERM, [](int) { gRunning.store(false); });

  Receiver rx;
  if (!rx.start(p))
    return 1;

  std::thread rxThread([&rx] { rx.run(); });

  int exitCode = 0;

  if (p.play) {
    // Auf das erste gueltige Paket warten (Format-Handshake).
    while (gRunning.load() && rx.sampleRate_.load() == 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(5));

    const int sr = rx.sampleRate_.load();
    if (sr > 0 && gRunning.load()) {
      // Low-Latency-Anforderung an den Pulse-Server (~10 ms tlength).
      const size_t ch = size_t(rx.channels_.load());
      pa_sample_spec ss{};
      ss.format = PA_SAMPLE_FLOAT32LE;
      ss.rate = uint32_t(sr);
      ss.channels = uint8_t(ch);

      pa_buffer_attr attr{};
      attr.maxlength = uint32_t(-1);
      attr.tlength = uint32_t(sr) * uint32_t(ch) * sizeof(float) / 100;
      attr.prebuf = uint32_t(-1);
      attr.minreq = uint32_t(-1);
      attr.fragsize = uint32_t(-1);

      int err = 0;
      pa_simple *s = pa_simple_new(
          nullptr, // Standard-Server
          "udp-audio-receiver",
          PA_STREAM_PLAYBACK,
          p.sink.empty() ? nullptr : p.sink.c_str(),
          "udp-stream",
          &ss, nullptr, // Standard-Kanalzuordnung
          &attr, &err);
      if (!s) {
        std::fprintf(stderr, "[ERROR] pa_simple_new: %s\n",
                     pa_strerror(err));
        gRunning.store(false);
        exitCode = 1;
      } else {
        std::thread playThread([&rx, s] { rx.playbackLoop(s); });
        while (gRunning.load())
          std::this_thread::sleep_for(std::chrono::milliseconds(200));
        playThread.join();
        pa_simple_free(s);
      }
    }
  }

  while (gRunning.load())
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

  rxThread.join();
  if (rx.fd() >= 0)
    ::close(rx.fd());

  std::printf("\n=== Zusammenfassung ===\n");
  rx.printStats("[FINAL]");
  return exitCode;
}
