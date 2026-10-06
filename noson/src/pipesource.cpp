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
#include <pipewire/loop.h>
#include <pipewire/core.h>
#include <pipewire/node.h>
#include <spa/param/audio/format-utils.h>
#include <spa/param/props.h>
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

struct PipeWireTap
{
  std::atomic<bool> active;
  struct spa_ringbuffer ring;
  char* mem;
  int efd;
};

struct PipeWireRuntime
{
  struct pw_main_loop* loop;
  struct pw_stream* stream;
  struct spa_audio_info format;
  std::atomic<bool> negotiated;
  std::atomic<bool> streaming;
  std::atomic<uint64_t> buffers;
  PipeWireSource* source;
  bool sinkNode;
  // lock-free PCM handoff (RT producer, drain consumer)
  struct spa_ringbuffer ring;
  char* ringmem;
  int efd;
  std::atomic<unsigned> dropped;
  // virtual-sink taps: fixed slots, sink-owned memory (never freed
  // while the sink lives, so the RT callback needs no locks)
  PipeWireTap taps[PW_MAX_TAPS];
  std::atomic_flag tapLock;
  // OS volume of the sink node (Plasma slider): linear 1.0 == 100%.
  // Updated from Props param events (loop thread), read by the volume
  // worker and by drains for PCM unscaling. Defaults to full scale.
  std::atomic<float> nodeVolume;
  std::atomic<bool> nodeMute;
  int vefd; // signalled on every Props volume/mute change
  // node-proxy Props subscription (sink mode): our stream's param_changed
  // does not deliver Props, so we subscribe on our own node directly.
  struct pw_registry* registry;
  struct pw_proxy* nodeProxy;
  struct spa_hook nodeListener;
};

class PipeWireLoop : private OS::Thread
{
public:
  PipeWireLoop(const std::string& name, const AudioFormat& format,
               const std::string& target, bool sinkMode,
               const std::string& sinkDesc, PipeWireRuntime* rt)
  : OS::Thread(), m_name(name), m_format(format), m_target(target)
  , m_sinkMode(sinkMode), m_sinkDesc(sinkDesc), m_rt(rt) { }
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
  std::string m_name;
  AudioFormat m_format;
  std::string m_target;
  bool m_sinkMode;
  std::string m_sinkDesc;
  PipeWireRuntime* m_rt;
};

class PipeWireDrain : private OS::Thread
{
public:
  PipeWireDrain(PipeWireSource* source, struct spa_ringbuffer* ring,
                char* mem, uint32_t size, int efd)
  : OS::Thread(), m_source(source), m_ring(ring), m_mem(mem)
  , m_size(size), m_efd(efd) { }
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
  int drainAvailable(char* buf, int bite, int channels);
  PipeWireSource* m_source;
  struct spa_ringbuffer* m_ring;
  char* m_mem;
  uint32_t m_size;
  int m_efd;
};

// Volume worker: debounces sink-node Props changes off the loop thread
// and forwards the slider position to the registered handler (which maps
// it onto the Sonos speaker volume). Never runs in realtime context.
class PipeWireVolumeSync : private OS::Thread
{
public:
  PipeWireVolumeSync(PipeWireVirtualSink* sink, PipeWireRuntime* rt)
  : OS::Thread(), m_sink(sink), m_rt(rt)
  , m_lastVol(-1.0f), m_lastMute(false), m_haveLast(false) { }
  virtual ~PipeWireVolumeSync() override
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
  PipeWireVirtualSink* m_sink;
  PipeWireRuntime* m_rt;
  float m_lastVol;
  bool m_lastMute;
  bool m_haveLast;
};

void* PipeWireVolumeSync::process()
{
  // Normal-priority worker: wait for slider changes, debounce, forward.
  while (!OS::Thread::is_stopped())
  {
    uint64_t cnt = 0;
    ssize_t r = read(m_rt->vefd, &cnt, sizeof(cnt));
    if (r <= 0)
    {
      if (errno == EINTR)
        continue;
      if (errno == EAGAIN)
      {
        usleep(5000);
        continue;
      }
      break; // vefd closed or stopped
    }
    // Coalesce rapid drags: settle 150ms before forwarding.
    usleep(150 * 1000);
    if (OS::Thread::is_stopped())
      break;
    // Drain any further pending signals (still dragging).
    uint64_t more = 0;
    while (read(m_rt->vefd, &more, sizeof(more)) > 0) { }
    float v = m_rt->nodeVolume.load(std::memory_order_relaxed);
    bool m = m_rt->nodeMute.load(std::memory_order_relaxed);
    if (m_haveLast && v == m_lastVol && m == m_lastMute)
      continue;
    m_lastVol = v;
    m_lastMute = m;
    m_haveLast = true;
    PipeWireVirtualSink::VolumeHandler h;
    {
      std::lock_guard<std::mutex> lk(m_sink->m_volMutex);
      h = m_sink->m_volHandler;
    }
    if (h)
      h(v, m);
    else
      DBG(DBG_INFO, "PipeWire: sink volume %.3f mute=%d (no handler)\n", v, (int)m);
  }
  return nullptr;
}

// Plasma slider moved: Props carry "volume" (float array, linear,
// 1.0 == 100%) and "mute" (bool). Runs on the loop thread, so only
// store atomics + signal; the volume worker does the rest.
static void sink_props_changed(PipeWireRuntime* rt, const struct spa_pod* param)
{
  if (!spa_pod_is_object(param))
    return;
  const struct spa_pod_object* obj =
      reinterpret_cast<const struct spa_pod_object*>(param);
  struct spa_pod_prop* prop = nullptr;
  bool changed = false;
  SPA_POD_OBJECT_FOREACH(obj, prop)
  {
    float nv = -1.0f;
    if (prop->key == SPA_PROP_volume && spa_pod_is_float(&prop->value))
    {
      // Scalar combined volume (this is what the slider drives).
      nv = reinterpret_cast<const struct spa_pod_float*>(&prop->value)->value;
    }
    else if (prop->key == SPA_PROP_channelVolumes &&
             spa_pod_is_array(&prop->value))
    {
      const struct spa_pod* arr = &prop->value;
      uint32_t n = SPA_POD_ARRAY_N_VALUES(arr);
      if (n > 0 && SPA_POD_ARRAY_VALUE_SIZE(arr) == sizeof(float))
      {
        const float* v =
            reinterpret_cast<const float*>(SPA_POD_ARRAY_VALUES(arr));
        nv = v[0];
        for (uint32_t i = 1; i < n; ++i)
          if (v[i] > nv)
            nv = v[i];
      }
    }
    if (nv >= 0.0f)
    {
      if (nv != rt->nodeVolume.load(std::memory_order_relaxed))
      {
        rt->nodeVolume.store(nv, std::memory_order_relaxed);
        changed = true;
      }
    }
    else if (prop->key == SPA_PROP_mute && spa_pod_is_bool(&prop->value))
    {
      bool nm = reinterpret_cast<const struct spa_pod_bool*>(&prop->value)->value != 0;
      if (nm != rt->nodeMute.load(std::memory_order_relaxed))
      {
        rt->nodeMute.store(nm, std::memory_order_relaxed);
        changed = true;
      }
    }
  }
  if (changed)
    DBG(DBG_INFO, "PipeWire: sink volume %.3f mute=%d\n",
        rt->nodeVolume.load(), (int)rt->nodeMute.load());
  if (changed && rt->vefd >= 0)
  {
    uint64_t one = 1;
    (void)write(rt->vefd, &one, sizeof(one));
  }
}

static void on_node_param(void* userdata, int seq, uint32_t id,
                          uint32_t index, uint32_t next,
                          const struct spa_pod* param)
{
  (void)seq; (void)index; (void)next;
  PipeWireRuntime* rt = static_cast<PipeWireRuntime*>(userdata);
  if (id == SPA_PARAM_Props && rt->sinkNode)
    sink_props_changed(rt, param);
}

static const struct pw_node_events node_events = {
  PW_VERSION_NODE_EVENTS,
  .info = nullptr,
  .param = on_node_param,
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

static int64_t drain_mono_ms()
{
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

// RT-safe: copy PCM into a lock-free ring, drop newest on overflow.
static void ring_feed(struct spa_ringbuffer* ring, char* mem, uint32_t size,
                      int efd, std::atomic<unsigned>* dropped,
                      const char* data, uint32_t len)
{
  if (len == 0 || len >= size)
    return;
  uint32_t idx = 0;
  int32_t fill = spa_ringbuffer_get_write_index(ring, &idx);
  uint32_t avail = size - (uint32_t)fill;
  if (len > avail)
  {
    if (dropped)
    {
      unsigned d = dropped->fetch_add(1) + 1;
      if ((d % 500) == 1)
        DBG(DBG_WARN, "PipeWire: ring overrun, dropped %u chunks\n", d);
    }
    return;
  }
  spa_ringbuffer_write_data(ring, mem, size, idx & (size - 1), data, len);
  spa_ringbuffer_write_update(ring, idx + len);
  if (efd >= 0)
  {
    uint64_t one = 1;
    (void)write(efd, &one, sizeof(one)); // EFD_NONBLOCK: never blocks
  }
}

static void ring_write(PipeWireRuntime* rt, const char* data, uint32_t len)
{
  ring_feed(&rt->ring, rt->ringmem, PW_RING_SIZE, rt->efd, &rt->dropped,
            data, len);
}

// Claim a free virtual-sink tap (spinlock only around find-and-claim).
static int tap_attach(PipeWireRuntime* rt)
{
  int idx = -1;
  while (rt->tapLock.test_and_set(std::memory_order_acquire)) { }
  for (int i = 0; i < PW_MAX_TAPS; ++i)
  {
    if (!rt->taps[i].active.load(std::memory_order_relaxed))
    {
      idx = i;
      break;
    }
  }
  if (idx >= 0)
  {
    // Reset tap indices while inactive (RT skips inactive taps, and tap
    // memory outlives sessions, so this is race-free).
    spa_ringbuffer_init(&rt->taps[idx].ring);
    rt->taps[idx].active.store(true, std::memory_order_release);
  }
  rt->tapLock.clear(std::memory_order_release);
  return idx;
}

static void tap_release(PipeWireRuntime* rt, int idx)
{
  if (idx < 0 || idx >= PW_MAX_TAPS)
    return;
  // Tap memory is sink-owned and outlives sessions, so the RT callback
  // can never touch freed memory; release is a plain flag store.
  rt->taps[idx].active.store(false, std::memory_order_release);
  if (rt->taps[idx].efd >= 0)
  {
    uint64_t one = 1;
    (void)write(rt->taps[idx].efd, &one, sizeof(one));
  }
}

// Slider tracking: streams don't get Props param_changed, so subscribe on
// our own node directly (volume + mute for the slider-to-speaker
// mapping). Deferred until the server assigns our node id (connect is
// async); runs on the loop thread via the driver tick.
static void ensure_props_subscription(PipeWireRuntime* rt)
{
  if (rt->nodeProxy || !rt->stream)
    return;
  uint32_t nid = pw_stream_get_node_id(rt->stream);
  if (nid == PW_ID_ANY)
    return;
  if (!rt->registry)
  {
    struct pw_core* core = pw_stream_get_core(rt->stream);
    if (core)
      rt->registry = pw_core_get_registry(core, PW_VERSION_REGISTRY, 0);
    if (!rt->registry)
      return;
  }
  rt->nodeProxy = (struct pw_proxy*)pw_registry_bind(
      rt->registry, nid, PW_TYPE_INTERFACE_Node, PW_VERSION_NODE, 0);
  if (!rt->nodeProxy)
    return;
  pw_node_add_listener((struct pw_node*)rt->nodeProxy,
                       &rt->nodeListener, &node_events, rt);
  uint32_t ids[] = { SPA_PARAM_Props };
  int res = pw_node_subscribe_params((struct pw_node*)rt->nodeProxy, ids, 1);
  DBG(DBG_INFO, "PipeWire: subscribed node %u Props (res=%d)\n", nid, res);
}

// Graph driver tick (loop thread): when our sink node is elected driver
// (i.e. no hardware runs), pump the graph so players linked to the node
// get scheduled even with everything else suspended. Thread-safe: the
// resulting process() runs in the RT thread as usual.
static void driver_tick(void* userdata, uint64_t expirations)
{
  (void)expirations;
  PipeWireRuntime* rt = static_cast<PipeWireRuntime*>(userdata);
  ensure_props_subscription(rt);
  if (rt->stream && pw_stream_is_driving(rt->stream))
    pw_stream_trigger_process(rt->stream);
}

static AudioFormat defaultFormat()
{  AudioFormat fmt;
  int rate = PW_DEFAULT_RATE;
  int channels = PW_DEFAULT_CHANNELS;
  const char* er = std::getenv("NOSON_PW_RATE");
  const char* ec = std::getenv("NOSON_PW_CHANNELS");
  if (er && atoi(er) > 0)
    rate = atoi(er);
  if (ec && atoi(ec) > 0 && atoi(ec) <= 8)
    channels = atoi(ec);
  // Fixed low-latency friendly format: S16LE 48k stereo. PipeWire resamples/converts.
  fmt.byteOrder = AudioFormat::LittleEndian;
  fmt.sampleType = AudioFormat::SignedInt;
  fmt.sampleSize = 16;
  fmt.sampleBytes = 0;
  fmt.sampleRate = (uint32_t)rate;
  fmt.channelCount = (uint8_t)channels;
  fmt.codec = "audio/pcm";
  return fmt;
}

}

PipeWireSource::PipeWireSource(const std::string& name, const std::string& target)
: AudioSource()
, m_name(name)
, m_target(target)
, m_format(defaultFormat())
, m_output(nullptr)
, m_blankKiller(&PCMBlankKillerS16LE)
, m_ownsRt(true)
, m_tapIdx(-1)
, m_p(nullptr)
, m_drain(nullptr)
, m_rt(nullptr)
{
  if (m_target.empty())
    m_target = env_or("NOSON_PW_TARGET", "");
}

PipeWireSource::PipeWireSource(PipeWireRuntime* sharedRt, int tapIdx,
                               const AudioFormat& format)
: AudioSource()
, m_name("noson-tap")
, m_target()
, m_format(format)
, m_output(nullptr)
, m_blankKiller(&PCMBlankKillerS16LE)
, m_ownsRt(false)
, m_tapIdx(tapIdx)
, m_p(nullptr)
, m_drain(nullptr)
, m_rt(sharedRt)
{
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
  if (m_drain || m_p)
    stop();
  m_output = out;
  if (m_ownsRt)
  {
    PipeWireRuntime* rt = new PipeWireRuntime();
    memset(rt, 0, sizeof(*rt));
    rt->source = this;
    rt->sinkNode = false;
    rt->negotiated.store(false);
    rt->streaming.store(false);
    rt->dropped.store(0);
    rt->tapLock.clear();
    rt->nodeVolume.store(1.0f);
    rt->nodeMute.store(false);
    rt->vefd = -1; // own-capture has no volume worker
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
    m_p = new PipeWireLoop(m_name, m_format, m_target, false, "", rt);
    m_drain = new PipeWireDrain(this, &rt->ring, rt->ringmem,
                                PW_RING_SIZE, rt->efd);
    m_p->start();
    m_drain->start();
    // Wait briefly for format negotiation (max 5s). Audio accumulates
    // in the ring meanwhile; nothing is lost.
    for (int i = 0; i < 50 && !rt->negotiated.load(); ++i)
      usleep(100 * 1000);
  }
  else
  {
    // Attach mode: feed comes from a virtual-sink tap owned by the sink.
    if (!m_rt || m_tapIdx < 0 || m_tapIdx >= PW_MAX_TAPS)
      return;
    PipeWireTap* tap = &m_rt->taps[m_tapIdx];
    m_drain = new PipeWireDrain(this, &tap->ring, tap->mem,
                                PW_TAP_RING_SIZE, tap->efd);
    m_drain->start();
  }
}

void PipeWireSource::stop()
{
  if (!m_drain && !m_p)
    return;
  int feedEfd = -1;
  if (m_drain)
  {
    // Wake the drain first so it can't block forever in read()
    m_drain->requestInterruption();
    if (m_ownsRt)
      feedEfd = (m_rt && m_rt->efd >= 0) ? m_rt->efd : -1;
    else if (m_tapIdx >= 0 && m_rt)
      feedEfd = m_rt->taps[m_tapIdx].efd;
    if (feedEfd >= 0)
    {
      uint64_t one = 1;
      (void)write(feedEfd, &one, sizeof(one));
    }
  }
  // Quit the pipewire main loop from this thread (thread-safe)
  if (m_p)
  {
    if (m_rt && m_rt->loop)
      pw_main_loop_quit(m_rt->loop);
    m_p->requestInterruption();
  }
  if (m_drain)
  {
    m_drain->waitFinished();
    delete m_drain;
    m_drain = nullptr;
  }
  if (m_p)
  {
    m_p->waitFinished();
    delete m_p;
    m_p = nullptr;
  }
  if (m_ownsRt && m_rt)
  {
    if (m_rt->efd >= 0)
      close(m_rt->efd);
    delete[] m_rt->ringmem;
    if (m_rt->dropped.load())
      DBG(DBG_WARN, "PipeWire: total dropped chunks: %u\n", m_rt->dropped.load());
    delete m_rt;
    m_rt = nullptr;
  }
  else if (!m_ownsRt && m_rt && m_tapIdx >= 0)
  {
    tap_release(m_rt, m_tapIdx);
    m_tapIdx = -1;
    m_rt = nullptr;
  }
  m_output = nullptr;
}

PipeWireVirtualSink::PipeWireVirtualSink(const std::string& nodeName,
                                         const std::string& description)
: m_name(nodeName.empty() ? "noson" : nodeName)
, m_desc(description.empty() ? "Sonos" : description)
, m_format(defaultFormat())
, m_rt(nullptr)
, m_loop(nullptr)
, m_volSync(nullptr)
{
}

PipeWireVirtualSink::~PipeWireVirtualSink()
{
  stop();
}

bool PipeWireVirtualSink::start()
{
  if (m_loop)
    return true;
  if (!PipeWireSource::IsAvailable())
    return false;
  PipeWireRuntime* rt = new PipeWireRuntime();
  memset(rt, 0, sizeof(*rt));
  rt->source = nullptr;
  rt->sinkNode = true;
  rt->negotiated.store(false);
  rt->streaming.store(false);
  rt->dropped.store(0);
  rt->tapLock.clear();
  rt->nodeVolume.store(1.0f);
  rt->nodeMute.store(false);
  rt->vefd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (rt->vefd < 0)
  {
    delete rt;
    return false;
  }
  for (int i = 0; i < PW_MAX_TAPS; ++i)
  {
    rt->taps[i].active.store(false);
    spa_ringbuffer_init(&rt->taps[i].ring);
    rt->taps[i].mem = new char[PW_TAP_RING_SIZE];
    rt->taps[i].efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (rt->taps[i].efd < 0)
    {
      for (int j = 0; j <= i; ++j)
      {
        delete[] rt->taps[j].mem;
        if (rt->taps[j].efd >= 0)
          close(rt->taps[j].efd);
      }
      delete rt;
      return false;
    }
  }
  m_rt = rt;
  m_loop = new PipeWireLoop(m_name, m_format, "", true, m_desc, rt);
  m_loop->start();
  m_volSync = new PipeWireVolumeSync(this, rt);
  m_volSync->start();
  for (int i = 0; i < 50 && !rt->negotiated.load(); ++i)
    usleep(100 * 1000);
  if (!m_loop->isRunning())
  {
    stop();
    return false;
  }
  DBG(DBG_INFO, "PipeWire: virtual sink '%s' (%s) ready\n",
      m_name.c_str(), m_desc.c_str());
  return true;
}

void PipeWireVirtualSink::stop()
{
  if (!m_loop)
    return;
  if (m_rt && m_rt->loop)
    pw_main_loop_quit(m_rt->loop);
  m_loop->requestInterruption();
  if (m_volSync)
    m_volSync->requestInterruption();
  if (m_rt && m_rt->vefd >= 0)
  {
    // Wake the volume worker so it can't block forever in read().
    uint64_t one = 1;
    (void)write(m_rt->vefd, &one, sizeof(one));
  }
  m_loop->waitFinished();
  delete m_loop;
  m_loop = nullptr;
  if (m_volSync)
  {
    m_volSync->waitFinished();
    delete m_volSync;
    m_volSync = nullptr;
  }
  if (m_rt)
  {
    for (int i = 0; i < PW_MAX_TAPS; ++i)
    {
      delete[] m_rt->taps[i].mem;
      if (m_rt->taps[i].efd >= 0)
        close(m_rt->taps[i].efd);
    }
    if (m_rt->vefd >= 0)
      close(m_rt->vefd);
    delete m_rt;
    m_rt = nullptr;
  }
}

void PipeWireVirtualSink::setVolumeHandler(VolumeHandler h)
{
  std::lock_guard<std::mutex> lk(m_volMutex);
  m_volHandler = h;
}

bool PipeWireVirtualSink::isRunning() const
{
  return m_loop && m_loop->isRunning();
}

bool PipeWireVirtualSink::hasFreeTap() const
{
  if (!isRunning() || !m_rt)
    return false;
  for (int i = 0; i < PW_MAX_TAPS; ++i)
    if (!m_rt->taps[i].active.load(std::memory_order_acquire))
      return true;
  return false;
}

int PipeWireVirtualSink::attachTap()
{
  if (!isRunning() || !m_rt)
    return -1;
  return tap_attach(m_rt);
}

void* PipeWireLoop::process()
{
  pw_global_init();
  PipeWireRuntime* rt = m_rt;

  std::string target = m_target;
  if (!m_sinkMode && target.empty())
    target = env_or("NOSON_PW_TARGET", "");
  const std::string latency = env_or("NOSON_PW_LATENCY", PW_DEFAULT_LATENCY);
  std::string captureSink;
  if (!m_sinkMode)
  {
    captureSink = env_or("NOSON_PW_CAPTURE_SINK", "");
    if (captureSink.empty() && target.empty())
      captureSink = "true"; // default: capture default sink monitor (desktop output)
  }

  rt->loop = pw_main_loop_new(nullptr);
  if (!rt->loop)
  {
    DBG(DBG_ERROR, "PipeWire: pw_main_loop_new failed\n");
    return nullptr;
  }

  struct pw_properties* props;
  if (m_sinkMode)
  {
    // Virtual sink: shows up in the OS sound settings as a regular
    // output device. Apps playing to it feed our taps; nothing is
    // audible locally and nothing needs manual wiring.
    props = pw_properties_new(
        PW_KEY_MEDIA_TYPE, "Audio",
        PW_KEY_MEDIA_CATEGORY, "Playback",
        PW_KEY_MEDIA_ROLE, "Music",
        PW_KEY_MEDIA_CLASS, "Audio/Sink",
        PW_KEY_NODE_NAME, m_name.c_str(),
        PW_KEY_NODE_DESCRIPTION, m_sinkDesc.c_str(),
        PW_KEY_NODE_NICK, m_sinkDesc.c_str(),
        PW_KEY_DEVICE_API, "noson",
        PW_KEY_DEVICE_CLASS, "sound",
        PW_KEY_DEVICE_DESCRIPTION, m_sinkDesc.c_str(),
        PW_KEY_DEVICE_ICON_NAME, "audio-speakers",
        PW_KEY_NODE_LATENCY, latency.c_str(),
        PW_KEY_NODE_VIRTUAL, "true",
        PW_KEY_NODE_DRIVER, "true",
        nullptr);
  }
  else
  {
    props = pw_properties_new(
        PW_KEY_MEDIA_TYPE, "Audio",
        PW_KEY_MEDIA_CATEGORY, "Capture",
        PW_KEY_MEDIA_ROLE, "Music",
        PW_KEY_NODE_LATENCY, latency.c_str(),
        PW_KEY_NODE_NAME, m_name.c_str(),
        nullptr);
    if (!target.empty())
      pw_properties_set(props, PW_KEY_TARGET_OBJECT, target.c_str());
    if (!captureSink.empty())
      pw_properties_set(props, PW_KEY_STREAM_CAPTURE_SINK, captureSink.c_str());
  }

  rt->stream = pw_stream_new_simple(
      pw_main_loop_get_loop(rt->loop),
      m_name.c_str(),
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
  raw.rate = m_format.sampleRate;
  raw.channels = m_format.channelCount;
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
      (pw_stream_flags)((m_sinkMode ? (PW_STREAM_FLAG_DRIVER) : PW_STREAM_FLAG_AUTOCONNECT) |
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

  // Sink mode drives its own graph cycles (5ms) so linked players get
  // scheduled even when all hardware is suspended. When real hardware
  // runs it wins driver election and we just follow.
  struct spa_source* timer = nullptr;
  struct pw_loop* ploop = nullptr;
  if (m_sinkMode)
  {
    ploop = pw_main_loop_get_loop(rt->loop);
    timer = pw_loop_add_timer(ploop, driver_tick, rt);
    if (timer)
    {
      struct timespec val = {0, 5000000};
      struct timespec iv = {0, 5000000};
      pw_loop_update_timer(ploop, timer, &val, &iv, false);
    }
    // Slider tracking is set up lazily from the driver tick (node id is
    // only valid once the server creates the node).
    rt->registry = nullptr;
    rt->nodeProxy = nullptr;
  }

  DBG(DBG_INFO, "PipeWire: %s target='%s' latency=%s rate=%u ch=%u\n",
      m_sinkMode ? "sink node" : "capturing",
      m_sinkMode ? m_name.c_str() : (target.empty() ? "<default>" : target.c_str()),
      latency.c_str(), m_format.sampleRate, m_format.channelCount);

  rt->streaming.store(true);
  pw_main_loop_run(rt->loop);
  rt->streaming.store(false);

  if (timer && ploop)
    pw_loop_destroy_source(ploop, timer);
  if (rt->nodeProxy)
  {
    pw_proxy_destroy(rt->nodeProxy);
    rt->nodeProxy = nullptr;
  }
  if (rt->registry)
  {
    pw_proxy_destroy((struct pw_proxy*)rt->registry);
    rt->registry = nullptr;
  }
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
  int64_t nextSilence = 0; // paced silence-fill deadline (mono ms)
  while (!OS::Thread::is_stopped())
  {
    uint64_t cnt = 0;
    ssize_t r = read(m_efd, &cnt, sizeof(cnt));
    if (r <= 0)
    {
      if (errno == EINTR)
        continue;
      if (errno != EAGAIN)
        break; // efd closed or stopped
      // No signal: ring may still hold data (coalesced wakeups).
    }
    if (OS::Thread::is_stopped())
      break;
    int produced = drainAvailable(buf, bite, channels);
    if (produced == 0)
    {
      // Ring dry (e.g. virtual sink with no players yet): emit paced
      // silence like a PulseAudio monitor does, so downstream (HTTP)
      // never starves and the speaker stays in steady PLAYING.
      int64_t now = drain_mono_ms();
      if (now >= nextSilence)
      {
        memset(buf, 0, bite);
        m_source->m_blankKiller(buf, channels, PW_DRAIN_FRAMES / 4);
        OutputStream* out = m_source->m_output;
        if (out && out->Write(buf, bite) != bite)
          break;
        nextSilence = now + (1000 * PW_DRAIN_FRAMES) / m_source->m_format.sampleRate;
      }
      else
        usleep(1000);
    }
  }
  // flush whatever remains (partial bite allowed at shutdown)
  drainAvailable(buf, bite, channels);
  delete[] buf;
  return nullptr;
}

int PipeWireDrain::drainAvailable(char* buf, int bite, int channels)
{
  OutputStream* out = m_source->m_output;
  if (!out || m_efd < 0)
    return 0;
  const int bpf = channels * 2; // S16LE bytes per frame
  int produced = 0;
  for (;;)
  {
    uint32_t idx = 0;
    int32_t avail = spa_ringbuffer_get_read_index(m_ring, &idx);
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
    spa_ringbuffer_read_data(m_ring, m_mem, m_size,
                             idx & (m_size - 1), buf, (uint32_t)len);
    spa_ringbuffer_read_update(m_ring, idx + len);
    if (m_source->m_mute)
      memset(buf, 0, len);
    else if (len / bpf >= 2)
      m_source->m_blankKiller(buf, channels, (len / bpf) / 4);
    // (a 1-frame tail skips the killer: it unconditionally touches
    // 2 frames, which would overflow a 1-frame buffer)
    // Unscale the sink-node soft volume: PipeWire applies the Plasma
    // slider as digital gain before our buffers arrive, but the slider
    // is forwarded to the Sonos speaker itself (volume worker), so the
    // stream must stay at full scale. Gain restores it within 1 LSB.
    if (m_source->m_rt)
    {
      float v = m_source->m_rt->nodeVolume.load(std::memory_order_relaxed);
      if (v >= 0.001f && v < 0.999f)
      {
        float g = 1.0f / v;
        int16_t* s = reinterpret_cast<int16_t*>(buf);
        int n = len / 2;
        for (int i = 0; i < n; ++i)
        {
          int32_t x = (int32_t)(s[i] * g);
          if (x > 32767) x = 32767;
          else if (x < -32768) x = -32768;
          s[i] = (int16_t)x;
        }
      }
    }
    if (out->Write(buf, len) != len)
    {
      DBG(DBG_ERROR, "PipeWire: write() failed\n");
      break;
    }
    produced += len;
    if (OS::Thread::is_stopped())
      break;
  }
  return produced;
}


void NSROOT::on_param_changed(void* userdata, uint32_t id, const struct spa_pod* param)
{
  PipeWireRuntime* rt = static_cast<PipeWireRuntime*>(userdata);
  if (param == nullptr)
    return;
  if (id == SPA_PARAM_Props && rt->sinkNode)
  {
    // Kept for setups that do deliver Props on streams; normally the
    // node-proxy subscription below handles this.
    sink_props_changed(rt, param);
    return;
  }
  if (id != SPA_PARAM_Format)
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
    // First-buffer marker (fires once): proves the graph delivers audio.
    if (rt->buffers.fetch_add(1) == 0)
      DBG(DBG_INFO, "PipeWire: first audio buffer, %u bytes%s\n", size,
          rt->sinkNode ? " (sink)" : "");
    uint32_t fmt = rt->format.info.raw.format;
    if (fmt == SPA_AUDIO_FORMAT_S16_LE || fmt == SPA_AUDIO_FORMAT_S16)
    {
      if (rt->sinkNode)
      {
        // Virtual sink: tee to every active tap (apps must never stall).
        for (int t = 0; t < PW_MAX_TAPS; ++t)
        {
          PipeWireTap* tap = &rt->taps[t];
          if (!tap->active.load(std::memory_order_acquire))
            continue;
          ring_feed(&tap->ring, tap->mem, PW_TAP_RING_SIZE, tap->efd,
                    nullptr, static_cast<const char*>(data), size);
        }
      }
      else
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
          const uint32_t bytes = n * ch * 2;
          if (rt->sinkNode)
          {
            for (int t = 0; t < PW_MAX_TAPS; ++t)
            {
              PipeWireTap* tap = &rt->taps[t];
              if (!tap->active.load(std::memory_order_acquire))
                continue;
              ring_feed(&tap->ring, tap->mem, PW_TAP_RING_SIZE, tap->efd,
                        nullptr, reinterpret_cast<const char*>(tmp), bytes);
            }
          }
          else
            ring_write(rt, reinterpret_cast<const char*>(tmp), bytes);
          done += n;
        }
      }
    }
    // else: unsupported format, drop (negotiation requested S16 so rare)
  }
  pw_stream_queue_buffer(rt->stream, pwbuf);
}
