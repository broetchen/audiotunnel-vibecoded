// ===========================================================================
//  udp_audio_source.cpp   (C++17, Linux, PipeWire)
//
//  UDP-Audio-Empfaenger fuer den WASAPI-Loopback-Streamer.
//  Empfaengt 16-Bit-PCM-Pakete mit 16-Byte-Header und stellt sie als
//  EIGENSTAENDIGES VIRTUELLES EINGABEGERAET (PipeWire-Source, Audio/Source)
//  bereit - nicht als Wiedergabe-Stream.
//
//  Unterschiede zum Vorbild (udp_audio_receiver.cpp / libpulse):
//    * kein pa_simple, kein Playback-Thread -> native PipeWire-API
//    * Datenfluss ist PULL: der RT-process-Callback des pw_stream leert den
//      Ringpuffer (Producer = UDP-Thread, Consumer = RT-Thread)
//    * PW_STREAM_FLAG_DRIVER: die Source hat eine eigene Clock und laeuft
//      auch dann, wenn gerade niemand aufnimmt
//    * Unterlauf -> Stille (statt Blockieren), damit die Zeitlinie laeuft
//
//  Protokoll (Little-Endian, identisch zum Sender):
//    uint32 magic ('LUMO'), uint16 sequence, uint16 payloadBytes,
//    uint32 sampleRate, uint16 channels, uint16 bitsPerSample
//
//  Build:
//    g++ -O2 -march=native -std=c++17 udp_audio_source.cpp \
//        -o udp_mic $(pkg-config --cflags --libs libpipewire-0.3) -lpthread
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

#include <pipewire/main-loop.h>
#include <pipewire/pipewire.h>
#include <pipewire/stream.h>
#include <spa/param/audio/format-utils.h>
#include <spa/pod/builder.h>

namespace {

// -------------------------------------------------------------- Protokoll --
constexpr uint32_t kMagic = 0x4F4D554Cu; // 'LUMO' little-endian
constexpr size_t kHeaderSize = 16;
constexpr int kDefaultPort = 5000;

constexpr double kDefaultPrebufferMs = 60.0;
constexpr unsigned kMaxPadPackets = 40; // Stille-Einschub bei Verlust

constexpr int kRecvBufBytes = 1 << 21;                   // 2 MiB Kernel-RCVBUF
constexpr size_t kRingCapacityFrames = size_t(1) << 18;  // ~5.5 s @ 48 kHz
constexpr size_t kScratchFloats = 32768;                 // max. Payload/2
constexpr unsigned kQuantumFrames = 512;                 // ~10 ms @ 48 kHz

// ------------------------------------------------------------------ Zeit ---
inline double monoNow() {
  return std::chrono::duration<double>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// ------------------------------------------------- lock-freier SPSC-Ring --
// Ein Producer (Rx-Thread), ein Consumer (PipeWire-RT-Thread). Indizes laufen
// monoton und werden per acquire/release gepaart -> kein Mutex, kein Alloc.
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
    return wIdx_.load(std::memory_order_acquire) -
           rIdx_.load(std::memory_order_acquire);
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
  float gain = 1.0f;
  double prebufferMs = kDefaultPrebufferMs;
  std::string nodeName = "udp-audio-source";
  std::string nodeDesc = "UDP Audio Input";
  std::string mediaRole = "Communication";
  bool driver = false; // Source als Graph-Treiber (eigene Clock)
  bool quiet = false;
};

std::atomic<bool> gRunning{true};
std::atomic<uint64_t> gUnderruns{0}; // vom RT-Thread hochgezaehlt

// -------------------------------------------------------------- Receiver ---
class Receiver {
public:
  bool start(const Params &p);
  void run(); // Empfangs-Thread

  // Format-Handshake: Rx-Thread publiziert, Main-Thread konsumiert.
  bool formatReady() const {
    return sampleRate_.load(std::memory_order_acquire) > 0;
  }
  int sampleRate() const { return sampleRate_.load(std::memory_order_acquire); }
  int channels() const { return channels_.load(std::memory_order_acquire); }

  RingBuffer &ring() { return ring_; }
  size_t ringBuffered() const { return ring_.buffered(); }
  int fd() const { return sock_; }

  void printStats(const char *prefix) const;

private:
  void checkSequence_(uint16_t seq);

  Params p_;
  int sock_ = -1;
  std::vector<uint8_t> dg_;    // Empfangs-Datagramm
  RingBuffer ring_;
  std::vector<float> scratch_; // konvertierte Samples eines Pakets
  std::vector<float> zeros_;   // Stille-Puffer fuer Paketverluste

  std::atomic<uint64_t> packetsOk_{0}, packetsBad_{0}, packetsLost_{0};
  std::atomic<uint64_t> packetsOoo_{0}, bytesRx_{0};

  std::atomic<int> sampleRate_{0};
  std::atomic<int> channels_{0};

  uint16_t framesPerPacket_ = 0;
  double startedAt_ = 0.0;
  uint16_t lastSeq_ = 0;
  bool seqInit_ = false;
  int badLogged_ = 0;
  int fmtLogged_ = 0;
};

bool Receiver::start(const Params &p) {
  p_ = p;
  dg_.resize(65536);
  scratch_.resize(kScratchFloats);
  zeros_.assign(size_t(kMaxPadPackets) * kScratchFloats, 0.0f);

  sock_ = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (sock_ < 0) {
    perror("socket");
    return false;
  }

  int one = 1;
  setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  setsockopt(sock_, SOL_SOCKET, SO_RCVBUF, &kRecvBufBytes, sizeof(kRecvBufBytes));
  timeval tv{0, 250000}; // 250 ms -> kontrollierter Thread-Exit
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
    // Stille einschieben (max. 40 Pakete), damit die Zeitlinie weiterlaeuft.
    const size_t padFrames =
        std::min<uint32_t>(delta, kMaxPadPackets) * framesPerPacket_;
    ring_.push(zeros_.data(), padFrames);
  }
  lastSeq_ = seq;
}

void Receiver::run() {
  if (!p_.quiet)
    std::printf("[INFO] Hoere auf UDP %s:%d ...\n", p_.bindAddr.c_str(),
                p_.port);

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
    const uint32_t magic = uint32_t(h[0]) | (uint32_t(h[1]) << 8) |
                           (uint32_t(h[2]) << 16) | (uint32_t(h[3]) << 24);
    const uint16_t seq = uint16_t(h[4]) | (uint16_t(h[5]) << 8);
    const uint16_t pb = uint16_t(h[6]) | (uint16_t(h[7]) << 8);
    const uint32_t sampleRate = uint32_t(h[8]) | (uint32_t(h[9]) << 8) |
                                (uint32_t(h[10]) << 16) | (uint32_t(h[11]) << 24);
    const uint16_t channels = uint16_t(h[12]) | (uint16_t(h[13]) << 8);
    const uint16_t bits = uint16_t(h[14]) | (uint16_t(h[15]) << 8);

    if (magic != kMagic || bits != 16 || channels < 1 || channels > 8 ||
        sampleRate < 8000 || sampleRate > 384000 || pb == 0 ||
        (pb % (2u * channels)) != 0 || size_t(n) < kHeaderSize + pb) {
      packetsBad_++;
      if (badLogged_ < 5) {
        char head[kHeaderSize * 3 + 1] = {0};
        for (size_t i = 0; i < kHeaderSize; ++i)
          std::snprintf(head + i * 3, 4, "%02X ", h[i]);
        std::fprintf(stderr,
                     "[WARN] Verworfenes Paket von %s:%u (%zd B) Header: %s\n",
                     inet_ntoa(from.sin_addr), ntohs(from.sin_port), n, head);
        badLogged_++;
      }
      continue;
    }

    // ---------- Erstes gueltiges Paket bestimmt das Format ----------
    // Reihenfolge ist wichtig: Ring zuerst initialisieren, danach das Format
    // per release publizieren -> wer rate != 0 sieht, sieht auch den Ring.
    if (!formatReady()) {
      ring_.init(kRingCapacityFrames, channels);
      channels_.store(int(channels), std::memory_order_release);
      sampleRate_.store(int(sampleRate), std::memory_order_release);
      startedAt_ = monoNow();
      if (!p_.quiet)
        std::printf(
            "[INFO] Stream-Format erkannt: %u Hz, %u Kanal/Kanaele, 16 Bit\n",
            sampleRate, channels);
    } else if (int(sampleRate) != sampleRate_ ||
               int(channels) != channels_) {
      // Formatwechsel mitten im Stream: ignorieren (die Source ist bereits
      // mit festem Format angelegt).
      packetsBad_++;
      if (fmtLogged_ < 3) {
        std::fprintf(stderr,
                     "[WARN] Formatwechsel ignoriert: %u Hz/%u ch "
                     "(aktiv: %d Hz/%d ch)\n",
                     sampleRate, channels, sampleRate_.load(),
                     channels_.load());
        fmtLogged_++;
      }
      continue;
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
    if (!p_.quiet && ok % 6000 == 0)
      printStats("[STATS]");
  }

  gRunning.store(false, std::memory_order_release);
}

void Receiver::printStats(const char *prefix) const {
  const double elapsed = startedAt_ ? monoNow() - startedAt_ : 0.0;
  const double kbps =
      elapsed > 0.0 ? double(bytesRx_.load()) * 8.0 / elapsed / 1000.0 : 0.0;
  std::printf("%s Pakete: %" PRIu64 " | verloren: %" PRIu64
              " | ungueltig: %" PRIu64 " | Out-of-order: %" PRIu64
              " | Unterlaeufe: %" PRIu64 " | Puffer: %zu Frames | %.1f kb/s\n",
              prefix, packetsOk_.load(), packetsLost_.load(),
              packetsBad_.load(), packetsOoo_.load(), gUnderruns.load(),
              ring_.buffered(), kbps);
  std::fflush(stdout);
}

// ------------------------------------------------------- PipeWire-Source ---
struct SourceData {
  Receiver *rx = nullptr;
  Params *p = nullptr;
  struct pw_stream *stream = nullptr;
  uint32_t channels = 0;
  size_t stride = 0; // Bytes pro Frame (channels * sizeof(float))
};

// RT-Thread: genau ein pop() + memcpy, keine Allokation, kein Lock.
void onProcess(void *userdata) {
  SourceData *d = static_cast<SourceData *>(userdata);
  struct pw_buffer *b = pw_stream_dequeue_buffer(d->stream);
  if (b == nullptr)
    return;

  struct spa_buffer *sb = b->buffer;
  uint8_t *base = static_cast<uint8_t *>(sb->datas[0].data);
  if (base == nullptr || sb->datas[0].chunk == nullptr) {
    pw_stream_queue_buffer(d->stream, b);
    return;
  }

  const uint32_t stride = static_cast<uint32_t>(d->stride);
  const uint32_t maxFrames = sb->datas[0].maxsize / stride;
  uint32_t want = static_cast<uint32_t>(b->requested);
  if (want == 0 || want > maxFrames)
    want = maxFrames;

  const size_t got = d->rx->ring().pop(reinterpret_cast<float *>(base), want);
  if (got < want) {
    // Unterlauf: Rest mit Stille fuellen, damit die Zeitlinie weiterlaeuft.
    std::memset(base + got * stride, 0, (want - got) * stride);
    gUnderruns.fetch_add(1, std::memory_order_relaxed);
  }

  sb->datas[0].chunk->offset = 0;
  sb->datas[0].chunk->stride = static_cast<int32_t>(stride);
  sb->datas[0].chunk->size = static_cast<uint32_t>(want * stride);

  pw_stream_queue_buffer(d->stream, b);
}

void onStateChanged(void *userdata, enum pw_stream_state /*old*/,
                    enum pw_stream_state state, const char *error) {
  SourceData *d = static_cast<SourceData *>(userdata);
  switch (state) {
  case PW_STREAM_STATE_ERROR:
    std::fprintf(stderr, "[ERROR] PipeWire-Stream: %s\n",
                 error ? error : "unbekannt");
    gRunning.store(false, std::memory_order_release);
    break;
  case PW_STREAM_STATE_UNCONNECTED:
    if (!d->p->quiet)
      std::printf("[INFO] Source vom Server getrennt\n");
    break;
  case PW_STREAM_STATE_PAUSED:
    if (!d->p->quiet)
      std::printf("[INFO] Source angemeldet (idle): '%s'\n",
                  d->p->nodeName.c_str());
    break;
  case PW_STREAM_STATE_STREAMING:
    if (!d->p->quiet)
      std::printf("[INFO] Source aktiv als Eingabegeraet: '%s'\n",
                  d->p->nodeName.c_str());
    break;
  default:
    break;
  }
  std::fflush(stdout);
}

int runPipeWire(Receiver &rx, const Params &p) {
  const int rate = rx.sampleRate();
  const int ch = rx.channels();

  pw_init(nullptr, nullptr); // kein Arg-Parsing durch PipeWire

  struct pw_main_loop *loop = pw_main_loop_new(nullptr);
  if (loop == nullptr) {
    std::fprintf(stderr, "[ERROR] pw_main_loop_new fehlgeschlagen\n");
    pw_deinit();
    return 1;
  }

  SourceData data;
  data.rx = &rx;
  data.p = const_cast<Params *>(&p);
  data.channels = static_cast<uint32_t>(ch);
  data.stride = size_t(ch) * sizeof(float);

  // Quantum der Source: node.latency = "<frames>/<rate>"
  char latency[64];
  std::snprintf(latency, sizeof(latency), "%u/%d", kQuantumFrames, rate);

  // ---- Diese Properties machen aus dem Stream ein Eingabegeraet. ----
  struct pw_properties *props = pw_properties_new(
      PW_KEY_MEDIA_TYPE, "Audio",
      PW_KEY_MEDIA_CATEGORY, "Capture",         // Quelle, nicht Senke
      PW_KEY_MEDIA_ROLE, p.mediaRole.c_str(),
      PW_KEY_MEDIA_CLASS, "Audio/Source",       // erscheint als Input-Device
      PW_KEY_NODE_NAME, p.nodeName.c_str(),
      PW_KEY_NODE_DESCRIPTION, p.nodeDesc.c_str(),
      PW_KEY_NODE_LATENCY, latency,
      PW_KEY_APP_NAME, "udp-audio-source",
      nullptr);

  struct pw_stream_events events;
  std::memset(&events, 0, sizeof(events));
  events.version = PW_VERSION_STREAM_EVENTS;
  events.state_changed = onStateChanged;
  events.process = onProcess;

  data.stream =
      pw_stream_new_simple(pw_main_loop_get_loop(loop), p.nodeName.c_str(),
                           props, &events, &data);
  if (data.stream == nullptr) {
    std::fprintf(stderr, "[ERROR] pw_stream_new_simple fehlgeschlagen\n");
    pw_main_loop_destroy(loop);
    pw_deinit();
    return 1;
  }

  // ---- Format: fix, weil es der Sender vorgibt ----
  uint8_t podBuf[1024];
  struct spa_pod_builder b = SPA_POD_BUILDER_INIT(podBuf, sizeof(podBuf));
  struct spa_audio_info_raw info;
  std::memset(&info, 0, sizeof(info));
  info.format = SPA_AUDIO_FORMAT_F32;
  info.rate = static_cast<uint32_t>(rate);
  info.channels = static_cast<uint32_t>(ch);
  if (ch == 1) {
    info.position[0] = SPA_AUDIO_CHANNEL_MONO;
  } else if (ch == 2) {
    info.position[0] = SPA_AUDIO_CHANNEL_FL;
    info.position[1] = SPA_AUDIO_CHANNEL_FR;
  } // sonst: Positionen unbekannt -> PipeWire nimmt die Standardreihenfolge

  const struct spa_pod *params[1];
  params[0] = spa_format_audio_raw_build(&b, SPA_PARAM_EnumFormat, &info);

  pw_stream_flags flags = static_cast<pw_stream_flags>(
      PW_STREAM_FLAG_MAP_BUFFERS | PW_STREAM_FLAG_RT_PROCESS);
  if (p.driver)
    flags = static_cast<pw_stream_flags>(flags | PW_STREAM_FLAG_DRIVER);
  // Bewusst KEIN PW_STREAM_FLAG_AUTOCONNECT: eine Quelle soll nicht
  // automatisch irgendwohin verlinkt werden.

  if (pw_stream_connect(data.stream, PW_DIRECTION_OUTPUT, PW_ID_ANY, flags,
                        params, 1) < 0) {
    std::fprintf(stderr, "[ERROR] pw_stream_connect fehlgeschlagen\n");
    pw_stream_destroy(data.stream);
    pw_main_loop_destroy(loop);
    pw_deinit();
    return 1;
  }

  if (!p.quiet)
    std::printf("[INFO] Virtuelle Source '%s' angelegt (%d Hz, %d ch, "
                "Treiber=%s)\n",
                p.nodeName.c_str(), rate, ch, p.driver ? "ja" : "nein");

  // ---- Watchdog: beendet die Main-Loop, wenn gRunning faellt. ----
  // pw_main_loop_quit() ist thread-safe, deshalb kein Signal-Handler-Trick.
  std::thread watchdog([loop] {
    while (gRunning.load(std::memory_order_relaxed))
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    pw_main_loop_quit(loop);
  });

  pw_main_loop_run(loop); // blockiert bis quit

  gRunning.store(false, std::memory_order_release);
  watchdog.join();

  pw_stream_destroy(data.stream);
  pw_main_loop_destroy(loop);
  pw_deinit();
  return 0;
}

// ------------------------------------------------------------------- CLI ---
void onSignal(int) { gRunning.store(false, std::memory_order_release); }

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
          "Usage: udp_mic [--host ADDR] [--port N] [--gain F]\n"
          "               [--prebuffer-ms F] [--source-name NAME]\n"
          "               [--description TEXT] [--role ROLE]\n"
          "               [--no-driver] [-q]\n"
          "\n"
          "  Erzeugt eine virtuelle PipeWire-Source (Eingabegeraet).\n"
          "  Pruefen:  wpctl status | pactl list short sources\n"
          "  Aufnehmen: pw-record --target NAME test.wav\n");
      std::exit(0);
    } else if (a == "--host")
      p.bindAddr = next("--host");
    else if (a == "--port")
      p.port = std::atoi(next("--port"));
    else if (a == "--gain")
      p.gain = float(std::atof(next("--gain")));
    else if (a == "--prebuffer-ms")
      p.prebufferMs = std::atof(next("--prebuffer-ms"));
    else if (a == "--source-name")
      p.nodeName = next("--source-name");
    else if (a == "--description")
      p.nodeDesc = next("--description");
    else if (a == "--role")
      p.mediaRole = next("--role");
    else if (a == "--no-driver")
      p.driver = false;
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

  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  std::signal(SIGPIPE, SIG_IGN);

  Receiver rx;
  if (!rx.start(p))
    return 1;

  std::thread rxThread([&rx] { rx.run(); });

  // ---- Format-Handshake: erst nach dem ersten gueltigen Paket kennen wir
  //      Abtastrate/Kanalzahl und koennen die Source korrekt anlegen. ----
  while (gRunning.load(std::memory_order_relaxed) && !rx.formatReady())
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

  int rc = 0;
  if (gRunning.load(std::memory_order_relaxed) && rx.formatReady()) {
    // Vorlauf fuellen, damit die Source nicht sofort mit Stille beginnt.
    if (p.prebufferMs > 0.0) {
      const size_t pre =
          size_t(double(rx.sampleRate()) * p.prebufferMs / 1000.0);
      const double deadline = monoNow() + 3.0;
      while (gRunning.load(std::memory_order_relaxed) &&
             rx.ringBuffered() < pre && monoNow() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    rc = runPipeWire(rx, p);
  }

  gRunning.store(false, std::memory_order_release);
  rxThread.join();
  if (rx.fd() >= 0)
    ::close(rx.fd());

  std::printf("\n=== Zusammenfassung ===\n");
  rx.printStats("[FINAL]");
  return rc;
}
