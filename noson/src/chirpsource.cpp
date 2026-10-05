/*
 *      Copyright (C) 2026 Jean-Luc Barriere / bedasrv
 *
 *  In-process chirp test source (see chirpsource.h).
 */

#include "chirpsource.h"
#include "private/debug.h"
#include "private/os/threads/thread.h"

#include <cmath>
#include <cstring>
#include <ctime>
#include <unistd.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

// 48kHz stereo S16, 256-frame bites (same cadence as PASource).
#define CHIRP_RATE        48000
#define CHIRP_CHANNELS    2
#define CHIRP_FRAMES      256
// Pattern: 4.5s silence + 0.5s log chirp 500 -> 4000 Hz at -6dB.
#define CHIRP_PERIOD_MS   5000
#define CHIRP_LEN_MS      500
#define CHIRP_F0          500.0
#define CHIRP_F1          4000.0
#define CHIRP_AMP         0.5

using namespace NSROOT;

namespace NSROOT
{

static int64_t mono_ms()
{
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

class ChirpWorker : private OS::Thread
{
public:
  explicit ChirpWorker(ChirpSource* source);
  virtual ~ChirpWorker() override;

  bool isRunning() { return OS::Thread::is_running(); }
  void start() { OS::Thread::start_thread(true); }
  void requestInterruption() { OS::Thread::stop_thread(false); }
  bool waitFinished() { return OS::Thread::wait_thread((unsigned)-1); }

private:
  void* process() override;
  ChirpSource* m_source;
  unsigned m_maxChirps; // 0 = infinite
};

}

ChirpSource::ChirpSource(const std::string& name)
: AudioSource()
, m_name(name)
, m_format()
, m_output(nullptr)
, m_p(new ChirpWorker(this))
{
  m_format.byteOrder = AudioFormat::LittleEndian;
  m_format.sampleType = AudioFormat::SignedInt;
  m_format.sampleSize = 16;
  m_format.sampleBytes = 0;
  m_format.sampleRate = CHIRP_RATE;
  m_format.channelCount = CHIRP_CHANNELS;
  m_format.codec = "audio/pcm";
}

ChirpSource::~ChirpSource()
{
  stop();
  delete m_p;
}

void ChirpSource::play(OutputStream* out)
{
  if (m_p->isRunning())
    stop();
  m_output = out;
  m_p->start();
}

void ChirpSource::stop()
{
  if (m_p->isRunning())
  {
    m_p->requestInterruption();
    m_p->waitFinished();
    m_output = nullptr;
  }
}

ChirpWorker::ChirpWorker(ChirpSource* source)
: OS::Thread()
, m_source(source)
, m_maxChirps(0)
{
  const char* e = std::getenv("NOSON_CHIRP_COUNT");
  if (e && *e)
  {
    int v = atoi(e);
    if (v > 0 && v <= 1000)
      m_maxChirps = (unsigned)v;
  }
}

ChirpWorker::~ChirpWorker()
{
  if (is_running())
    stop_thread(true);
}

void* ChirpWorker::process()
{
  const int bpf = CHIRP_CHANNELS * 2;
  const int bite = bpf * CHIRP_FRAMES;
  char* buf = new char[bite];
  const int periodFrames = CHIRP_RATE * CHIRP_PERIOD_MS / 1000;
  const int chirpFrames = CHIRP_RATE * CHIRP_LEN_MS / 1000;
  const double k = log(CHIRP_F1 / CHIRP_F0); // log sweep constant
  int pos = 0; // position inside the 5s pattern
  unsigned chirpNo = 0;
  bool chirpLogged = false;
  // Pace production to wall clock so the stream stays realtime
  // (an unpaced generator would flood the ringbuffer and burn CPU).
  int64_t t0 = mono_ms();
  uint64_t framesOut = 0;
  while (!OS::Thread::is_stopped())
  {
    int16_t* s = reinterpret_cast<int16_t*>(buf);
    for (int f = 0; f < CHIRP_FRAMES; ++f)
    {
      int16_t v = 0;
      bool inChirp = pos >= periodFrames - chirpFrames
          && !(m_maxChirps && chirpNo >= m_maxChirps);
      if (inChirp)
      {
        int ct = pos - (periodFrames - chirpFrames); // 0 .. chirpFrames
        double t = (double)ct / CHIRP_RATE;
        double phase = 2.0 * M_PI * CHIRP_F0 * (CHIRP_LEN_MS / 1000.0) / k
                     * (exp(k * (double)ct / chirpFrames) - 1.0);
        (void)t;
        double env = 1.0;
        const int edge = CHIRP_RATE / 100; // 10ms raised-cosine edges
        if (ct < edge)
          env = 0.5 - 0.5 * cos(M_PI * ct / edge);
        else if (ct >= chirpFrames - edge)
          env = 0.5 - 0.5 * cos(M_PI * (chirpFrames - ct) / edge);
        v = (int16_t)(CHIRP_AMP * 32767.0 * env * sin(phase));
        if (!chirpLogged)
        {
          chirpLogged = true;
          ++chirpNo;
          timespec wall;
          clock_gettime(CLOCK_REALTIME, &wall);
          struct tm tmv;
          localtime_r(&wall.tv_sec, &tmv);
          DBG(DBG_INFO, "Chirp #%u start @ %lld ms (wall %02d:%02d:%02d.%03ld)\n",
              chirpNo, (long long)mono_ms(),
              tmv.tm_hour, tmv.tm_min, tmv.tm_sec, wall.tv_nsec / 1000000);
        }
      }
      else
      {
        chirpLogged = false;
      }
      for (int c = 0; c < CHIRP_CHANNELS; ++c)
        *s++ = v;
      if (++pos >= periodFrames)
        pos = 0;
    }
    if (m_source->m_output)
    {
      if (m_source->m_mute)
        memset(buf, 0, bite);
      if (m_source->m_output->Write(buf, bite) != bite)
      {
        DBG(DBG_ERROR, "Chirp: write() failed\n");
        break;
      }
    }
    framesOut += CHIRP_FRAMES;
    int64_t expect = t0 + (int64_t)(framesOut * 1000 / CHIRP_RATE);
    int64_t now = mono_ms();
    if (expect > now + 1)
      usleep((useconds_t)((expect - now) * 1000));
  }
  delete[] buf;
  return nullptr;
}
