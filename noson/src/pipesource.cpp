/*
 *      Copyright (C) 2026 Jean-Luc Barriere / bedasrv
 *
 *  PipeWire native low-latency capture source.
 *
 *  Threading mirrors upstream PASource: all blocking work (FLAC encode,
 *  ringbuffer mutex, wakeups) runs in a normal-priority drain thread.
 *  The PipeWire realtime process callback only memcpys PCM into a
 *  lock-free spa_ringbuffer and signals an eventfd. Doing anything
 *  heavier in the RT callback causes xruns (glitchy Sonos audio).
 */

#include "pipesource.h"
#include "private/debug.h"
#include "private/pcmblankkiller.h"
#include "private/os/threads/thread.h"
#include "private/os/threads/timeout.h"

#include <pipewire/pipewire.h>
#include <spa/param/audio/format-utils.h>
#include <spa/utils/ringbuffer.h>
#include <spa/utils/result.h>

#include <sys/eventfd.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>
#include <errno.h>

#define PW_DEFAULT_RATE      48000
#define PW_DEFAULT_CHANNELS  2
#define PW_DEFAULT_LATENCY   "256/48000"

// PCM ring between RT callback and drain thread (power of two).
// 256KB ~= 1.3s at S16LE 48k stereo; absorbs FLAC/HTTP stalls.
#define PW_RING_SIZE         262144
// Drain bite matches upstream PASource FRAME_BUFFER (256 frames).
#define PW_DRAIN_FRAMES      256

using namespace NSROOT;

namespace NSROOT
{

struct PipeWireRuntime
{
  struct pw_main_loop* loop;
  struct pw_stream* stream;
  struct spa_audio_info format;
  std::atomic<bool> negotiated;
  std::atomic<bool> streaming;
  PipeWireSource* source;
  // lock-free PCM handoff (RT producer, drain consumer)
  struct spa_ringbuffer ring;
  char* ringmem;
  int efd;
  std::atomic<unsigned> dropped;
};

class PipeWireLoop : private OS::Thread
{
public:
  explicit PipeWireLoop(PipeWireSource* source, PipeWireRuntime* rt)
  : OS::Thread(), m_source(source), m_rt(rt) { }
  virtual ~PipeWireLoop() override
  {
    if (is_running())
      stop_thread(true);
  }
  bool isRunning() { return OS::Thread::is_running(); }
  void start() { OS::Thread::start_thread(true); }
  void requestInterruption() { OS::Thread::stop_thread(false); }
  bool waitFinished() { return OS::Thread::wait_thread((unsigned)-1); }
private:
  void* process() override;
  PipeWireSource* m_source;
  PipeWireRuntime* m_rt;
};

class PipeWireDrain : private OS::Thread
{
public:
  explicit PipeWireDrain(PipeWireSource* source, PipeWireRuntime* rt)
  : OS::Thread(), m_source(source), m_rt(rt) { }
  virtual ~PipeWireDrain() override
  {
    if (is_running())
      stop_thread(true);
  }
  bool isRunning() { return OS::Thread::is_running(); }
  void start() { OS::Thread::start_thread(true); }
  void requestInterruption() { OS::Thread::stop_thread(false); }
  bool waitFinished() { return OS::Thread::wait_thread((unsigned)-1); }
private:
  void* process() override;
  void drainAvailable(char* buf, int bite, int channels);
  PipeWireSource* m_source;
  PipeWireRuntime* m_rt;
};

static const struct pw_stream_events stream_events = {
  PW_VERSION_STREAM_EVENTS,
  .destroy = nullptr,
  .state_changed = nullptr,
  .control_info = nullptr,
  .io_changed = nullptr,
  .param_changed = on_param_changed,
  .add_buffer = nullptr,
  .remove_buffer = nullptr,
  .process = on_process,
  .drained = nullptr,
  .command = nullptr,
  .trigger_done = nullptr,
};

static std::atomic<int> s_pwInitCount(0);
static void pw_global_init()
{
  int expected = 0;
  if (s_pwInitCount.compare_exchange_strong(expected, 1))
    pw_init(nullptr, nullptr);
  else
    s_pwInitCount.fetch_add(1);
}

static std::string env_or(const char* key, const std::string& fallback)
{
  const char* v = std::getenv(key);
  if (v && *v)
    return std::string(v);
  return fallback;
}

// RT-safe: copy PCM into the lock-free ring, drop newest on overflow.
static void ring_write(PipeWireRuntime* rt, const char* data, uint32_t len)
{
  if (len == 0 || len >= PW_RING_SIZE)
    return;
  uint32_t idx = 0;
  int32_t fill = spa_ringbuffer_get_write_index(&rt->ring, &idx);
  uint32_t avail = (uint32_t)PW_RING_SIZE - (uint32_t)fill;
  if (len > avail)
  {
    unsigned d = rt->dropped.fetch_add(1) + 1;
    if ((d % 500) == 1)
      DBG(DBG_WARN, "PipeWire: ring overrun, dropped %u chunks\n", d);
    return;
  }
  spa_ringbuffer_write_data(&rt->ring, rt->ringmem, PW_RING_SIZE,
                            idx & (PW_RING_SIZE - 1), data, len);
  spa_ringbuffer_write_update(&rt->ring, idx + len);
  uint64_t one = 1;
  (void)write(rt->efd, &one, sizeof(one)); // EFD_NONBLOCK: never blocks
}

}

PipeWireSource::PipeWireSource(const std::string& name, const std::string& target)
: AudioSource()
, m_name(name)
, m_target(target)
, m_format()
, m_output(nullptr)
, m_blankKiller(&PCMBlankKillerS16LE)
, m_p(nullptr)
, m_drain(nullptr)
{
  // Fixed low-latency friendly format: S16LE 48k stereo. PipeWire resamples/converts.
  int rate = PW_DEFAULT_RATE;
  int channels = PW_DEFAULT_CHANNELS;
  const char* er = std::getenv("NOSON_PW_RATE");
  const char* ec = std::getenv("NOSON_PW_CHANNELS");
  if (er && atoi(er) > 0)
    rate = atoi(er);
  if (ec && atoi(ec) > 0 && atoi(ec) <= 8)
    channels = atoi(ec);
  m_format.byteOrder = AudioFormat::LittleEndian;
  m_format.sampleType = AudioFormat::SignedInt;
  m_format.sampleSize = 16;
  m_format.sampleBytes = 0;
  m_format.sampleRate = (uint32_t)rate;
  m_format.channelCount = (uint8_t)channels;
  m_format.codec = "audio/pcm";
  if (m_target.empty())
    m_target = env_or("NOSON_PW_TARGET", "");
}

PipeWireSource::~PipeWireSource()
{
  stop();
}

bool PipeWireSource::IsAvailable()
{
  if (std::getenv("NOSON_PW_DISABLE"))
    return false;
  // Fast probe: pipewire socket must exist
  const char* rdir = std::getenv("XDG_RUNTIME_DIR");
  std::string sock;
  if (rdir)
    sock.assign(rdir).append("/pipewire-0");
  else
    sock.assign("/run/user/").append(std::to_string((long long)getuid())).append("/pipewire-0");
  const char* remote = std::getenv("PIPEWIRE_REMOTE");
  if (remote && *remote)
    sock = remote;
  if (access(sock.c_str(), F_OK) != 0)
    return false;
  return true;
}

void PipeWireSource::play(OutputStream* out)
{
  if (m_p)
    stop();
  m_output = out;
  PipeWireRuntime* rt = new PipeWireRuntime();
  memset(rt, 0, sizeof(*rt));
  rt->source = this;
  rt->negotiated.store(false);
  rt->streaming.store(false);
  rt->dropped.store(0);
  rt->ringmem = new char[PW_RING_SIZE];
  spa_ringbuffer_init(&rt->ring);
  rt->efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (rt->efd < 0)
  {
    DBG(DBG_ERROR, "PipeWire: eventfd failed\n");
    delete[] rt->ringmem;
    delete rt;
    m_output = nullptr;
    return;
  }
  m_rt = rt;
  m_p = new PipeWireLoop(this, rt);
  m_drain = new PipeWireDrain(this, rt);
  m_p->start();
  m_drain->start();
  // Wait briefly for format negotiation (max 5s). Audio accumulates
  // in the ring meanwhile; nothing is lost.
  for (int i = 0; i < 50 && !rt->negotiated.load(); ++i)
    usleep(100 * 1000);
}

void PipeWireSource::stop()
{
  if (!m_p)
    return;
  // Wake the drain first so it can't block forever in read()
  m_drain->requestInterruption();
  if (m_rt && m_rt->efd >= 0)
  {
    uint64_t one = 1;
    (void)write(m_rt->efd, &one, sizeof(one));
  }
  // Quit the pipewire main loop from this thread (thread-safe)
  if (m_rt && m_rt->loop)
    pw_main_loop_quit(m_rt->loop);
  m_p->requestInterruption();
  m_drain->waitFinished();
  m_p->waitFinished();
  if (m_rt)
  {
    if (m_rt->efd >= 0)
      close(m_rt->efd);
    delete[] m_rt->ringmem;
    if (m_rt->dropped.load())
      DBG(DBG_WARN, "PipeWire: total dropped chunks: %u\n", m_rt->dropped.load());
    delete m_rt;
    m_rt = nullptr;
  }
  delete m_drain;
  m_drain = nullptr;
  delete m_p;
  m_p = nullptr;
  m_output = nullptr;
}

void* PipeWireLoop::process()
{
  pw_global_init();
  PipeWireRuntime* rt = m_rt;

  const std::string target = m_source->m_target.empty()
      ? env_or("NOSON_PW_TARGET", "") : m_source->m_target;
  const std::string latency = env_or("NOSON_PW_LATENCY", PW_DEFAULT_LATENCY);
  std::string captureSink = env_or("NOSON_PW_CAPTURE_SINK", "");
  if (captureSink.empty() && target.empty())
    captureSink = "true"; // default: capture default sink monitor (desktop output)

  rt->loop = pw_main_loop_new(nullptr);
  if (!rt->loop)
  {
    DBG(DBG_ERROR, "PipeWire: pw_main_loop_new failed\n");
    return nullptr;
  }

  struct pw_properties* props = pw_properties_new(
      PW_KEY_MEDIA_TYPE, "Audio",
      PW_KEY_MEDIA_CATEGORY, "Capture",
      PW_KEY_MEDIA_ROLE, "Music",
      PW_KEY_NODE_LATENCY, latency.c_str(),
      PW_KEY_NODE_NAME, m_source->m_name.c_str(),
      nullptr);
  if (!target.empty())
    pw_properties_set(props, PW_KEY_TARGET_OBJECT, target.c_str());
  if (!captureSink.empty())
    pw_properties_set(props, PW_KEY_STREAM_CAPTURE_SINK, captureSink.c_str());

  rt->stream = pw_stream_new_simple(
      pw_main_loop_get_loop(rt->loop),
      m_source->m_name.c_str(),
      props,
      &stream_events,
      rt);
  if (!rt->stream)
  {
    DBG(DBG_ERROR, "PipeWire: pw_stream_new_simple failed\n");
    pw_main_loop_destroy(rt->loop);
    rt->loop = nullptr;
    return nullptr;
  }

  uint8_t buffer[1024];
  struct spa_pod_builder b = SPA_POD_BUILDER_INIT(buffer, sizeof(buffer));
  struct spa_audio_info_raw raw;
  raw.format = SPA_AUDIO_FORMAT_S16_LE;
  raw.rate = m_source->m_format.sampleRate;
  raw.channels = m_source->m_format.channelCount;
  raw.flags = 0;
  raw.position[0] = SPA_AUDIO_CHANNEL_FL;
  if (raw.channels > 1)
    raw.position[1] = SPA_AUDIO_CHANNEL_FR;
  for (uint32_t i = 2; i < raw.channels && i < SPA_AUDIO_MAX_CHANNELS; ++i)
    raw.position[i] = SPA_AUDIO_CHANNEL_UNKNOWN;

  const struct spa_pod* params[1];
  params[0] = spa_format_audio_raw_build(&b, SPA_PARAM_EnumFormat, &raw);

  int res = pw_stream_connect(rt->stream,
      PW_DIRECTION_INPUT,
      PW_ID_ANY,
      (pw_stream_flags)(PW_STREAM_FLAG_AUTOCONNECT |
                        PW_STREAM_FLAG_MAP_BUFFERS |
                        PW_STREAM_FLAG_RT_PROCESS),
      params, 1);
  if (res < 0)
  {
    DBG(DBG_ERROR, "PipeWire: pw_stream_connect failed: %s\n", spa_strerror(res));
    pw_stream_destroy(rt->stream);
    rt->stream = nullptr;
    pw_main_loop_destroy(rt->loop);
    rt->loop = nullptr;
    return nullptr;
  }

  DBG(DBG_INFO, "PipeWire: capturing target='%s' latency=%s rate=%u ch=%u\n",
      target.empty() ? "<default>" : target.c_str(), latency.c_str(),
      m_source->m_format.sampleRate, m_source->m_format.channelCount);

  rt->streaming.store(true);
  pw_main_loop_run(rt->loop);
  rt->streaming.store(false);

  if (rt->stream)
  {
    pw_stream_destroy(rt->stream);
    rt->stream = nullptr;
  }
  if (rt->loop)
  {
    pw_main_loop_destroy(rt->loop);
    rt->loop = nullptr;
  }
  return nullptr;
}

void* PipeWireDrain::process()
{
  // Normal-priority worker, mirrors PASourceWorker: blocking and heavy
  // work (FLAC encode, ringbuffer mutex, wakeups) is allowed here.
  const int bytesPerFrame = (int)m_source->m_format.channelCount * 2; // S16
  const int bite = bytesPerFrame * PW_DRAIN_FRAMES;
  char* buf = new char[bite];
  const int channels = m_source->m_format.channelCount;
  while (!OS::Thread::is_stopped())
  {
    uint64_t cnt = 0;
    ssize_t r = read(m_rt->efd, &cnt, sizeof(cnt));
    if (r <= 0)
    {
      if (errno == EINTR)
        continue;
      if (errno == EAGAIN)
      {
        usleep(1000);
        continue;
      }
      break; // efd closed or stopped
    }
    if (OS::Thread::is_stopped())
      break;
    drainAvailable(buf, bite, channels);
  }
  // flush whatever remains (partial bite allowed at shutdown)
  drainAvailable(buf, bite, channels);
  delete[] buf;
  return nullptr;
}

void PipeWireDrain::drainAvailable(char* buf, int bite, int channels)
{
  OutputStream* out = m_source->m_output;
  if (!out)
    return;
  const int bpf = channels * 2; // S16LE bytes per frame
  for (;;)
  {
    uint32_t idx = 0;
    int32_t avail = spa_ringbuffer_get_read_index(&m_rt->ring, &idx);
    if (avail <= 0)
      break;
    // process full bites; a partial tail waits for the next wakeup,
    // except at shutdown where it is drained in bite-sized steps
    int len = avail >= bite ? bite : avail;
    if (len < bite && !OS::Thread::is_stopped())
      break;
    len -= len % bpf; // whole frames only
    if (len <= 0)
      break; // cannot make progress on a sub-frame tail
    spa_ringbuffer_read_data(&m_rt->ring, m_rt->ringmem, PW_RING_SIZE,
                             idx & (PW_RING_SIZE - 1), buf, (uint32_t)len);
    spa_ringbuffer_read_update(&m_rt->ring, idx + len);
    if (m_source->m_mute)
      memset(buf, 0, len);
    else if (len / bpf >= 2)
      m_source->m_blankKiller(buf, channels, (len / bpf) / 4);
    // (a 1-frame tail skips the killer: it unconditionally touches
    // 2 frames, which would overflow a 1-frame buffer)
    if (out->Write(buf, len) != len)
    {
      DBG(DBG_ERROR, "PipeWire: write() failed\n");
      break;
    }
    if (OS::Thread::is_stopped())
      break;
  }
}

void NSROOT::on_param_changed(void* userdata, uint32_t id, const struct spa_pod* param)
{
  PipeWireRuntime* rt = static_cast<PipeWireRuntime*>(userdata);
  if (param == nullptr || id != SPA_PARAM_Format)
    return;
  uint32_t mt, st;
  if (spa_format_parse(param, &mt, &st) < 0)
    return;
  if (mt != SPA_MEDIA_TYPE_audio || st != SPA_MEDIA_SUBTYPE_raw)
    return;
  if (spa_format_audio_raw_parse(param, &rt->format.info.raw) < 0)
    return;
  DBG(DBG_INFO, "PipeWire: negotiated rate=%u ch=%u fmt=%u\n",
      rt->format.info.raw.rate, rt->format.info.raw.channels, rt->format.info.raw.format);
  rt->negotiated.store(true);
}

void NSROOT::on_process(void* userdata)
{
  // REALTIME thread: memcpy/arithmetic only. No encode, no mutex,
  // no condvar, no logging on the hot path.
  PipeWireRuntime* rt = static_cast<PipeWireRuntime*>(userdata);
  struct pw_buffer* pwbuf = pw_stream_dequeue_buffer(rt->stream);
  if (!pwbuf)
    return;
  struct spa_buffer* buf = pwbuf->buffer;
  void* data = buf && buf->n_datas > 0 ? buf->datas[0].data : nullptr;
  uint32_t size = buf && buf->n_datas > 0 && buf->datas[0].chunk
      ? buf->datas[0].chunk->size : 0;
  if (data && size && rt->negotiated.load())
  {
    uint32_t fmt = rt->format.info.raw.format;
    if (fmt == SPA_AUDIO_FORMAT_S16_LE || fmt == SPA_AUDIO_FORMAT_S16)
    {
      ring_write(rt, static_cast<const char*>(data), size);
    }
    else if (fmt == SPA_AUDIO_FORMAT_F32_LE || fmt == SPA_AUDIO_FORMAT_F32)
    {
      // Convert float32 [-1,1] to S16LE in small stack chunks
      const float* in = static_cast<const float*>(data);
      uint32_t ch = rt->format.info.raw.channels;
      if (ch > 0 && ch <= 8)
      {
        uint32_t frames = size / sizeof(float) / ch;
        int16_t tmp[PW_DRAIN_FRAMES * 8];
        uint32_t done = 0;
        while (done < frames)
        {
          uint32_t n = frames - done;
          if (n > PW_DRAIN_FRAMES)
            n = PW_DRAIN_FRAMES;
          for (uint32_t i = 0; i < n * ch; ++i)
          {
            float v = in[done * ch + i];
            if (v > 1.0f) v = 1.0f;
            if (v < -1.0f) v = -1.0f;
            tmp[i] = (int16_t)(v * 32767.0f);
          }
          ring_write(rt, reinterpret_cast<const char*>(tmp), n * ch * 2);
          done += n;
        }
      }
    }
    // else: unsupported format, drop (negotiation requested S16 so rare)
  }
  pw_stream_queue_buffer(rt->stream, pwbuf);
}
