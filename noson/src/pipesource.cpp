/*
 *      Copyright (C) 2026 Jean-Luc Barriere / bedasrv
 *
 *  PipeWire native low-latency capture source.
 */

#include "pipesource.h"
#include "private/debug.h"
#include "private/pcmblankkiller.h"
#include "private/os/threads/thread.h"
#include "private/os/threads/timeout.h"

#include <pipewire/pipewire.h>
#include <spa/param/audio/format-utils.h>
#include <spa/utils/result.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>

#define PW_DEFAULT_RATE      48000
#define PW_DEFAULT_CHANNELS  2
#define PW_DEFAULT_LATENCY   "256/48000"

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
};

class PipeWireWorker : private OS::Thread
{
public:
  explicit PipeWireWorker(PipeWireSource* source);
  virtual ~PipeWireWorker() override;

  bool isRunning() { return OS::Thread::is_running(); }
  void start() { OS::Thread::start_thread(true); }
  void requestInterruption() { OS::Thread::stop_thread(false); }
  bool waitFinished(unsigned ms) { return OS::Thread::wait_thread(ms); }
  bool waitFinished() { return OS::Thread::wait_thread((unsigned)-1); }

  PipeWireRuntime* rt() { return &m_rt; }

private:
  void* process() override;
  PipeWireSource* m_source;
  PipeWireRuntime m_rt;
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

}

PipeWireSource::PipeWireSource(const std::string& name, const std::string& target)
: AudioSource()
, m_name(name)
, m_target(target)
, m_format()
, m_output(nullptr)
, m_blankKiller(&PCMBlankKillerS16LE)
, m_p(new PipeWireWorker(this))
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
  delete m_p;
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
  {
    // Also accept pipewire-pulse socket as fallback indicator (compat mode)
    return false;
  }
  return true;
}

void PipeWireSource::play(OutputStream* out)
{
  if (m_p->isRunning())
    stop();
  m_output = out;
  m_p->start();
  // Wait briefly for format negotiation (max 5s). Stream still works if late:
  // on_process drops until negotiated.
  for (int i = 0; i < 50 && !m_p->rt()->negotiated.load(); ++i)
    usleep(100 * 1000);
}

void PipeWireSource::stop()
{
  if (m_p->isRunning())
  {
    // Quit the pipewire main loop from this thread (thread-safe)
    PipeWireRuntime* rt = m_p->rt();
    if (rt->loop)
      pw_main_loop_quit(rt->loop);
    m_p->requestInterruption();
    m_p->waitFinished();
    m_output = nullptr;
  }
}

PipeWireWorker::PipeWireWorker(PipeWireSource* source)
: OS::Thread()
, m_source(source)
{
  memset(&m_rt, 0, sizeof(m_rt));
  m_rt.source = source;
  m_rt.negotiated.store(false);
  m_rt.streaming.store(false);
}

PipeWireWorker::~PipeWireWorker()
{
  if (is_running())
    stop_thread(true);
}

void* PipeWireWorker::process()
{
  pw_global_init();

  const std::string target = m_source->m_target.empty()
      ? env_or("NOSON_PW_TARGET", "") : m_source->m_target;
  const std::string latency = env_or("NOSON_PW_LATENCY", PW_DEFAULT_LATENCY);
  std::string captureSink = env_or("NOSON_PW_CAPTURE_SINK", "");
  if (captureSink.empty() && target.empty())
    captureSink = "true"; // default: capture default sink monitor (desktop output)

  m_rt.loop = pw_main_loop_new(nullptr);
  if (!m_rt.loop)
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

  m_rt.format.media_type = 0;
  m_rt.stream = pw_stream_new_simple(
      pw_main_loop_get_loop(m_rt.loop),
      m_source->m_name.c_str(),
      props,
      &stream_events,
      &m_rt);
  if (!m_rt.stream)
  {
    DBG(DBG_ERROR, "PipeWire: pw_stream_new_simple failed\n");
    pw_main_loop_destroy(m_rt.loop);
    m_rt.loop = nullptr;
    return nullptr;
  }

  uint8_t buffer[1024];
  struct spa_pod_builder b = SPA_POD_BUILDER_INIT(buffer, sizeof(buffer));
  struct spa_audio_info_raw raw;
  raw.format = SPA_AUDIO_FORMAT_S16_LE;
  raw.rate = m_source->m_format.sampleRate;
  raw.channels = m_source->m_format.channelCount;
  // zero rest (flags, position) for default layout
  raw.flags = 0;
  raw.position[0] = SPA_AUDIO_CHANNEL_FL;
  if (raw.channels > 1)
    raw.position[1] = SPA_AUDIO_CHANNEL_FR;
  for (uint32_t i = 2; i < raw.channels && i < SPA_AUDIO_MAX_CHANNELS; ++i)
    raw.position[i] = SPA_AUDIO_CHANNEL_UNKNOWN;

  const struct spa_pod* params[1];
  params[0] = spa_format_audio_raw_build(&b, SPA_PARAM_EnumFormat, &raw);

  int res = pw_stream_connect(m_rt.stream,
      PW_DIRECTION_INPUT,
      PW_ID_ANY,
      (pw_stream_flags)(PW_STREAM_FLAG_AUTOCONNECT |
                        PW_STREAM_FLAG_MAP_BUFFERS |
                        PW_STREAM_FLAG_RT_PROCESS),
      params, 1);
  if (res < 0)
  {
    DBG(DBG_ERROR, "PipeWire: pw_stream_connect failed: %s\n", spa_strerror(res));
    pw_stream_destroy(m_rt.stream);
    m_rt.stream = nullptr;
    pw_main_loop_destroy(m_rt.loop);
    m_rt.loop = nullptr;
    return nullptr;
  }

  DBG(DBG_INFO, "PipeWire: capturing target='%s' latency=%s rate=%u ch=%u\n",
      target.empty() ? "<default>" : target.c_str(), latency.c_str(),
      m_source->m_format.sampleRate, m_source->m_format.channelCount);

  m_rt.streaming.store(true);
  pw_main_loop_run(m_rt.loop);
  m_rt.streaming.store(false);

  if (m_rt.stream)
  {
    pw_stream_destroy(m_rt.stream);
    m_rt.stream = nullptr;
  }
  if (m_rt.loop)
  {
    pw_main_loop_destroy(m_rt.loop);
    m_rt.loop = nullptr;
  }
  return nullptr;
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
  PipeWireRuntime* rt = static_cast<PipeWireRuntime*>(userdata);
  PipeWireSource* src = rt->source;
  struct pw_buffer* pwbuf = pw_stream_dequeue_buffer(rt->stream);
  if (!pwbuf)
    return;
  struct spa_buffer* buf = pwbuf->buffer;
  void* data = buf && buf->n_datas > 0 ? buf->datas[0].data : nullptr;
  uint32_t size = buf && buf->n_datas > 0 && buf->datas[0].chunk
      ? buf->datas[0].chunk->size : 0;
  OutputStream* out = src->m_output;
  if (data && size && out && rt->negotiated.load())
  {
    uint32_t fmt = rt->format.info.raw.format;
    if (fmt == SPA_AUDIO_FORMAT_S16_LE || fmt == SPA_AUDIO_FORMAT_S16)
    {
      int channels = src->m_format.channelCount;
      if (src->m_mute)
        memset(data, 0, size);
      else
        src->m_blankKiller(data, channels, (int)(size / (2 * channels) / 4));
      out->Write(static_cast<const char*>(data), (int)size);
    }
    else if (fmt == SPA_AUDIO_FORMAT_F32_LE || fmt == SPA_AUDIO_FORMAT_F32)
    {
      // Convert float32 [-1,1] to S16LE in place via temp buffer
      const float* in = static_cast<const float*>(data);
      uint32_t frames = size / sizeof(float) / rt->format.info.raw.channels;
      uint32_t ch = rt->format.info.raw.channels;
      // Stack-friendly chunked conversion; size is typically <= 8k
      int16_t tmp[8192];
      uint32_t done = 0;
      while (done < frames)
      {
        uint32_t n = frames - done;
        if (n > 8192 / ch)
          n = 8192 / ch;
        for (uint32_t i = 0; i < n * ch; ++i)
        {
          float v = in[done * ch + i];
          if (v > 1.0f) v = 1.0f;
          if (v < -1.0f) v = -1.0f;
          tmp[i] = (int16_t)(v * 32767.0f);
        }
        uint32_t bytes = n * ch * 2;
        if (src->m_mute)
          memset(tmp, 0, bytes);
        else
          src->m_blankKiller(tmp, (int)ch, (int)(n / 4));
        if (out->Write(reinterpret_cast<const char*>(tmp), (int)bytes) != (int)bytes)
          break;
        done += n;
      }
    }
    // else: unsupported format, drop
  }
  pw_stream_queue_buffer(rt->stream, pwbuf);
}
