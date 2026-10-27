// loopback_stream.cpp — WASAPI-Loopback-Capture -> UDP (16-Bit-PCM)
// MinGW:  g++ -std=c++17 -O2 -Wall loopback_stream.cpp -o loopback_stream.exe -lole32 -lws2_32
// MSVC:   cl /EHsc /O2 /std:c++17 loopback_stream.cpp

#define WIN32_LEAN_AND_MEAN

// MUSS VOR ALLEN ANDEREN HEADERN STEHEN
#include <audioclient.h>
#include <conio.h>
#include <mmdeviceapi.h>
#include <mmreg.h>
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#ifdef _MSC_VER
#pragma comment(lib, "ole32.lib")
#pragma comment(lib, "ws2_32.lib")
#endif

static const char *SERVER_IP = "192.168.178.42";
static const uint16_t SERVER_PORT = 5000;
static const size_t MAX_UDP_PAYLOAD = 1152;
static const double SILENCE_AFTER_MS = 60.0;
static const double MAX_PAD_MS = 300.0;

#pragma pack(push, 1)
struct StreamHeader {
  uint32_t magic;
  uint16_t sequence;
  uint16_t payloadBytes;
  uint32_t sampleRate;
  uint16_t channels;
  uint16_t bitsPerSample;
};
#pragma pack(pop)

static const uint32_t STREAM_MAGIC = 0x4F4D554Cu;

static const GUID kSubtypePcm =
    {0x00000001, 0x0000, 0x0010, {0x80, 0x00, 0x00, 0xaa, 0x00, 0x38, 0x9b, 0x71}};
static const GUID kSubtypeIeeeFloat =
    {0x00000003, 0x0000, 0x0010, {0x80, 0x00, 0x00, 0xaa, 0x00, 0x38, 0x9b, 0x71}};

enum class SampleKind { Unknown,
                        Float32,
                        Int16,
                        Int24,
                        Int32,
                        UInt8 };

static SampleKind ClassifyFormat(const WAVEFORMATEX *wf) {
  bool isFloat = false;

  if (wf->wFormatTag == WAVE_FORMAT_EXTENSIBLE && wf->cbSize >= 22) {
    const WAVEFORMATEXTENSIBLE *ext =
        reinterpret_cast<const WAVEFORMATEXTENSIBLE *>(wf);
    if (IsEqualGUID(ext->SubFormat, kSubtypeIeeeFloat))
      isFloat = true;
    else if (!IsEqualGUID(ext->SubFormat, kSubtypePcm))
      return SampleKind::Unknown;
  } else if (wf->wFormatTag == WAVE_FORMAT_IEEE_FLOAT) {
    isFloat = true;
  } else if (wf->wFormatTag != WAVE_FORMAT_PCM) {
    return SampleKind::Unknown;
  }

  if (isFloat)
    return (wf->wBitsPerSample == 32) ? SampleKind::Float32 : SampleKind::Unknown;

  switch (wf->wBitsPerSample) {
  case 16:
    return SampleKind::Int16;
  case 24:
    return SampleKind::Int24;
  case 32:
    return SampleKind::Int32;
  case 8:
    return SampleKind::UInt8;
  default:
    return SampleKind::Unknown;
  }
}

static void ConvertToInt16(SampleKind kind, const BYTE *src, size_t samples, int16_t *dst) {
  switch (kind) {
  case SampleKind::Float32: {
    const float *f = reinterpret_cast<const float *>(src);
    for (size_t i = 0; i < samples; ++i) {
      float v = f[i];
      if (!(v == v))
        v = 0.0f;
      v = (v < -1.0f) ? -1.0f : ((v > 1.0f) ? 1.0f : v);
      dst[i] = static_cast<int16_t>(std::lrintf(v * 32767.0f));
    }
    break;
  }
  case SampleKind::Int16:
    std::memcpy(dst, src, samples * sizeof(int16_t));
    break;
  case SampleKind::Int32: {
    const int32_t *s32 = reinterpret_cast<const int32_t *>(src);
    for (size_t i = 0; i < samples; ++i)
      dst[i] = static_cast<int16_t>(s32[i] >> 16);
    break;
  }
  case SampleKind::Int24:
    for (size_t i = 0; i < samples; ++i) {
      const BYTE *p = src + i * 3;
      uint32_t u = (static_cast<uint32_t>(p[2]) << 24) |
                   (static_cast<uint32_t>(p[1]) << 16) |
                   (static_cast<uint32_t>(p[0]) << 8);
      dst[i] = static_cast<int16_t>(static_cast<int32_t>(u) >> 16);
    }
    break;
  case SampleKind::UInt8:
    for (size_t i = 0; i < samples; ++i)
      dst[i] = static_cast<int16_t>((static_cast<int>(src[i]) - 128) << 8);
    break;
  default:
    std::memset(dst, 0, samples * sizeof(int16_t));
    break;
  }
}

class LoopbackStreamer {
public:
  bool InitNetwork();
  bool InitAudio();
  void Run();
  void RequestStop() { running_ = false; }
  void Shutdown();
  void PrintStats() const;

  uint32_t SampleRate() const { return sampleRate_; }
  uint16_t Channels() const { return channels_; }
  bool Finished() const { return finished_; }

private:
  void DrainPackets();
  void PadSilenceIfIdle();
  void QueueSamples(const int16_t *s, size_t n);
  void QueueSilence(size_t frames);
  void FlushFullDatagrams();
  void CompactTx();
  void SendDatagram(const int16_t *s, size_t n);

  IMMDevice *device_ = nullptr;
  IAudioClient *audioClient_ = nullptr;
  IAudioCaptureClient *captureClient_ = nullptr;
  HANDLE audioEvent_ = nullptr;
  WAVEFORMATEX *mixFormat_ = nullptr;
  SampleKind kind_ = SampleKind::Unknown;
  uint32_t sampleRate_ = 48000;
  uint16_t channels_ = 2;
  bool comReady_ = false;

  SOCKET sock_ = INVALID_SOCKET;
  sockaddr_in server_{};
  bool wsaReady_ = false;
  uint16_t seq_ = 0;

  std::vector<int16_t> tx_;
  std::vector<int16_t> scratch_;
  std::vector<char> packetBuf_;
  size_t txRead_ = 0;

  std::atomic<bool> running_{false};
  std::atomic<bool> finished_{false};
  std::chrono::steady_clock::time_point lastData_{};
  uint64_t sendErrors_ = 0;
  uint64_t discontinuities_ = 0;
  uint64_t packetsSent_ = 0;
  uint64_t bytesSent_ = 0;
};

bool LoopbackStreamer::InitNetwork() {
  WSADATA wsa{};
  int rc = WSAStartup(MAKEWORD(2, 2), &wsa);
  if (rc != 0) {
    std::cerr << "WSAStartup fehlgeschlagen: " << rc << "\n";
    return false;
  }
  wsaReady_ = true;

  sock_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if (sock_ == INVALID_SOCKET) {
    std::cerr << "socket() fehlgeschlagen: " << WSAGetLastError() << "\n";
    return false;
  }

  // Empfohlene Optionen für Audio-Streaming
  int sndbuf = 256 * 1024;
  setsockopt(sock_, SOL_SOCKET, SO_SNDBUF,
             reinterpret_cast<const char *>(&sndbuf), sizeof(sndbuf));

  DWORD sendTimeoutMs = 100;
  setsockopt(sock_, SOL_SOCKET, SO_SNDTIMEO,
             reinterpret_cast<const char *>(&sendTimeoutMs), sizeof(sendTimeoutMs));

  server_.sin_family = AF_INET;
  server_.sin_port = htons(SERVER_PORT);
  if (inet_pton(AF_INET, SERVER_IP, &server_.sin_addr) != 1) {
    std::cerr << "Ungueltige Server-IP: " << SERVER_IP << "\n";
    return false;
  }

  if (connect(sock_, reinterpret_cast<sockaddr *>(&server_), sizeof(server_)) == SOCKET_ERROR) {
    std::cerr << "connect() fehlgeschlagen: " << WSAGetLastError() << "\n";
    return false;
  }

  // QoS Priorität (optional, kann fehlschlagen)
  int tos = 0xB8;
  setsockopt(sock_, IPPROTO_IP, IP_TOS,
             reinterpret_cast<const char *>(&tos), sizeof(tos));

  return true;
}

bool LoopbackStreamer::InitAudio() {
  HRESULT hr = CoInitializeEx(nullptr, COINIT_MULTITHREADED);
  if (FAILED(hr) && hr != RPC_E_CHANGED_MODE) {
    std::cerr << "CoInitializeEx fehlgeschlagen: 0x" << std::hex << hr << "\n";
    return false;
  }
  comReady_ = true;

  IMMDeviceEnumerator *enumerator = nullptr;
  hr = CoCreateInstance(__uuidof(MMDeviceEnumerator), nullptr, CLSCTX_ALL,
                        IID_PPV_ARGS(&enumerator));
  if (FAILED(hr)) {
    std::cerr << "MMDeviceEnumerator fehlgeschlagen: 0x" << std::hex << hr << "\n";
    return false;
  }

  hr = enumerator->GetDefaultAudioEndpoint(eRender, eConsole, &device_);
  enumerator->Release();
  if (FAILED(hr)) {
    std::cerr << "Kein Standard-Wiedergabegeraet gefunden: 0x" << std::hex << hr << "\n";
    return false;
  }

  hr = device_->Activate(__uuidof(IAudioClient), CLSCTX_ALL, nullptr,
                         reinterpret_cast<void **>(&audioClient_));
  if (FAILED(hr)) {
    std::cerr << "IAudioClient-Aktivierung fehlgeschlagen: 0x" << std::hex << hr << "\n";
    return false;
  }

  hr = audioClient_->GetMixFormat(&mixFormat_);
  if (FAILED(hr)) {
    std::cerr << "GetMixFormat fehlgeschlagen: 0x" << std::hex << hr << "\n";
    return false;
  }

  kind_ = ClassifyFormat(mixFormat_);
  if (kind_ == SampleKind::Unknown) {
    std::cerr << "Nicht unterstuetztes Mix-Format\n";
    return false;
  }
  channels_ = mixFormat_->nChannels;
  sampleRate_ = mixFormat_->nSamplesPerSec;

  audioEvent_ = CreateEventW(nullptr, FALSE, FALSE, nullptr);
  if (!audioEvent_) {
    std::cerr << "CreateEvent fehlgeschlagen: " << GetLastError() << "\n";
    return false;
  }

  hr = audioClient_->Initialize(
      AUDCLNT_SHAREMODE_SHARED,
      AUDCLNT_STREAMFLAGS_LOOPBACK | AUDCLNT_STREAMFLAGS_EVENTCALLBACK,
      0, 0, mixFormat_, nullptr);
  if (FAILED(hr)) {
    std::cerr << "IAudioClient::Initialize fehlgeschlagen: 0x" << std::hex << hr << "\n";
    return false;
  }

  hr = audioClient_->SetEventHandle(audioEvent_);
  if (FAILED(hr)) {
    std::cerr << "SetEventHandle fehlgeschlagen: 0x" << std::hex << hr << "\n";
    return false;
  }

  hr = audioClient_->GetService(IID_PPV_ARGS(&captureClient_));
  if (FAILED(hr)) {
    std::cerr << "IAudioCaptureClient fehlgeschlagen: 0x" << std::hex << hr << "\n";
    return false;
  }

  return true;
}

void LoopbackStreamer::Run() {
  running_ = true;
  finished_ = false;

  HRESULT hr = CoInitializeEx(nullptr, COINIT_MULTITHREADED);
  const bool comHere = SUCCEEDED(hr) || hr == RPC_E_CHANGED_MODE;

  hr = audioClient_->Start();
  if (FAILED(hr)) {
    std::cerr << "IAudioClient::Start fehlgeschlagen: 0x" << std::hex << hr << "\n";
    finished_ = true;
    if (comHere)
      CoUninitialize();
    return;
  }

  lastData_ = std::chrono::steady_clock::now();

  while (running_) {
    DWORD w = WaitForSingleObject(audioEvent_, 10);
    if (w == WAIT_FAILED) {
      std::cerr << "WaitForSingleObject fehlgeschlagen: " << GetLastError() << "\n";
      break;
    }
    DrainPackets();
    PadSilenceIfIdle();
  }

  audioClient_->Stop();
  if (comHere)
    CoUninitialize();
  finished_ = true;
}

void LoopbackStreamer::DrainPackets() {
  UINT32 packetFrames = 0;

  while (SUCCEEDED(captureClient_->GetNextPacketSize(&packetFrames)) && packetFrames > 0) {
    BYTE *pData = nullptr;
    UINT32 frames = 0;
    DWORD flags = 0;
    UINT64 devPos = 0, qpc = 0;

    HRESULT hr = captureClient_->GetBuffer(&pData, &frames, &flags, &devPos, &qpc);
    if (FAILED(hr)) {
      std::cerr << "GetBuffer fehlgeschlagen: 0x" << std::hex << hr << "\n";
      break;
    }

    if (frames > 0) {
      if (flags & AUDCLNT_BUFFERFLAGS_DATA_DISCONTINUITY)
        ++discontinuities_;

      if (flags & AUDCLNT_BUFFERFLAGS_SILENT) {
        QueueSilence(frames);
      } else if (pData) {
        const size_t samples = static_cast<size_t>(frames) * channels_;
        scratch_.resize(samples);
        ConvertToInt16(kind_, pData, samples, scratch_.data());
        QueueSamples(scratch_.data(), samples);
      }
      lastData_ = std::chrono::steady_clock::now();
    }

    captureClient_->ReleaseBuffer(frames);
  }
}

void LoopbackStreamer::PadSilenceIfIdle() {
  const auto now = std::chrono::steady_clock::now();
  const double idleMs =
      std::chrono::duration<double, std::milli>(now - lastData_).count();

  if (idleMs <= SILENCE_AFTER_MS)
    return;

  double padMs = idleMs - SILENCE_AFTER_MS;
  if (padMs > MAX_PAD_MS)
    padMs = MAX_PAD_MS;

  const size_t frames =
      static_cast<size_t>(padMs * static_cast<double>(sampleRate_) / 1000.0);
  if (frames > 0)
    QueueSilence(frames);

  lastData_ = now;
}

void LoopbackStreamer::QueueSilence(size_t frames) {
  if (frames == 0)
    return;
  CompactTx();
  tx_.insert(tx_.end(), frames * channels_, int16_t{0});
  FlushFullDatagrams();
}

void LoopbackStreamer::QueueSamples(const int16_t *s, size_t n) {
  if (n == 0)
    return;
  CompactTx();
  tx_.insert(tx_.end(), s, s + n);
  FlushFullDatagrams();
}

void LoopbackStreamer::CompactTx() {
  if (txRead_ >= tx_.size()) {
    tx_.clear();
    txRead_ = 0;
    return;
  }
  if (txRead_ == 0)
    return;

  const size_t remaining = tx_.size() - txRead_;
  std::memmove(tx_.data(), tx_.data() + txRead_, remaining * sizeof(int16_t));
  tx_.resize(remaining);
  txRead_ = 0;
}

void LoopbackStreamer::FlushFullDatagrams() {
  const size_t samplesPerPacket = MAX_UDP_PAYLOAD / sizeof(int16_t);

  while (tx_.size() - txRead_ >= samplesPerPacket) {
    SendDatagram(tx_.data() + txRead_, samplesPerPacket);
    txRead_ += samplesPerPacket;
  }
}

void LoopbackStreamer::SendDatagram(const int16_t *samples, size_t n) {
  const size_t payloadBytes = n * sizeof(int16_t);

  StreamHeader header{};
  header.magic = STREAM_MAGIC;
  header.sequence = seq_++;
  header.payloadBytes = static_cast<uint16_t>(payloadBytes);
  header.sampleRate = sampleRate_;
  header.channels = channels_;
  header.bitsPerSample = 16;

  packetBuf_.resize(sizeof(StreamHeader) + payloadBytes);
  std::memcpy(packetBuf_.data(), &header, sizeof(header));
  std::memcpy(packetBuf_.data() + sizeof(header), samples, payloadBytes);

  const int len = send(sock_, packetBuf_.data(),
                       static_cast<int>(packetBuf_.size()), 0);
  if (len == SOCKET_ERROR) {
    ++sendErrors_;
    if (sendErrors_ <= 10 || (sendErrors_ % 500) == 0)
      std::cerr << "send() fehlgeschlagen (#" << sendErrors_
                << "): " << WSAGetLastError() << "\n";
  } else {
    ++packetsSent_;
    bytesSent_ += static_cast<uint64_t>(len);
  }
}

void LoopbackStreamer::PrintStats() const {
  const double seconds = (sampleRate_ > 0 && channels_ > 0)
                             ? static_cast<double>(bytesSent_) /
                                   (static_cast<double>(sampleRate_) * channels_ * 2.0)
                             : 0.0;

  std::cout << "\n=== Statistiken ===\n";
  std::cout << "Format:             " << sampleRate_ << " Hz, "
            << channels_ << " Kanaele, 16 Bit\n";
  std::cout << "Pakete gesendet:    " << packetsSent_ << "\n";
  std::cout << "Bytes gesendet:     " << bytesSent_ << "\n";
  std::cout << "Audio-Dauer ca.:    " << seconds << " s\n";
  std::cout << "Sendefehler:        " << sendErrors_ << "\n";
  std::cout << "Discontinuities:    " << discontinuities_ << "\n";
}

void LoopbackStreamer::Shutdown() {
  if (sock_ != INVALID_SOCKET) {
    closesocket(sock_);
    sock_ = INVALID_SOCKET;
  }
  if (wsaReady_) {
    WSACleanup();
    wsaReady_ = false;
  }

  if (captureClient_) {
    captureClient_->Release();
    captureClient_ = nullptr;
  }
  if (audioClient_) {
    audioClient_->Release();
    audioClient_ = nullptr;
  }
  if (device_) {
    device_->Release();
    device_ = nullptr;
  }
  if (mixFormat_) {
    CoTaskMemFree(mixFormat_);
    mixFormat_ = nullptr;
  }
  if (audioEvent_) {
    CloseHandle(audioEvent_);
    audioEvent_ = nullptr;
  }
  if (comReady_) {
    CoUninitialize();
    comReady_ = false;
  }
}

static std::atomic<bool> g_stopRequested{false};

static BOOL WINAPI ConsoleCtrlHandler(DWORD ctrlType) {
  switch (ctrlType) {
  case CTRL_C_EVENT:
  case CTRL_BREAK_EVENT:
  case CTRL_CLOSE_EVENT:
  case CTRL_LOGOFF_EVENT:
  case CTRL_SHUTDOWN_EVENT:
    g_stopRequested = true;
    return TRUE;
  default:
    return FALSE;
  }
}

int main() {
  SetConsoleTitleA("WASAPI Loopback Streamer");
  SetConsoleCtrlHandler(ConsoleCtrlHandler, TRUE);

  std::cout << "WASAPI Loopback Streamer\n";
  std::cout << "Ziel: " << SERVER_IP << ":" << SERVER_PORT << "\n\n";

  LoopbackStreamer streamer;

  if (!streamer.InitNetwork()) {
    std::cerr << "Netzwerk-Initialisierung fehlgeschlagen.\n";
    return 1;
  }
  if (!streamer.InitAudio()) {
    std::cerr << "Audio-Initialisierung fehlgeschlagen.\n";
    streamer.Shutdown();
    return 1;
  }

  std::cout << "Audio-Format: " << streamer.SampleRate() << " Hz, "
            << streamer.Channels() << " Kanaele, 16 Bit PCM\n";
  std::cout << "Stream laeuft. Enter oder Ctrl+C zum Beenden...\n\n";

  std::thread worker([&streamer]() { streamer.Run(); });

  while (!g_stopRequested && !streamer.Finished()) {
    if (_kbhit()) {
      const int c = _getch();
      if (c == '\r' || c == '\n')
        break;
    }
    Sleep(50);
  }

  streamer.RequestStop();
  if (worker.joinable())
    worker.join();

  streamer.PrintStats();
  streamer.Shutdown();

  return 0;
}
