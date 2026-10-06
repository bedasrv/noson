/*
 *      Copyright (C) 2026 Jean-Luc Barriere / bedasrv
 *
 *  PipeWire native low-latency capture sources, plus a persistent
 *  virtual sink ("Sonos" output device) so desktop audio can be routed
 *  to Sonos from the OS sound settings with no manual wiring.
 *  Target object can be forced via NOSON_PW_TARGET (node name or id),
 *  e.g. alsa_input.pci-0000_00_1f.3.analog-stereo for raw mic tests.
 *  Latency via NOSON_PW_LATENCY (default "256/48000").
 *  Virtual sink: NOSON_VIRTUAL_SINK=0 disables, NOSON_SINK_NAME renames.
 */

#ifndef PIPESOURCE_H
#define PIPESOURCE_H

#include "local_config.h"
#include "audiosource.h"

#include <atomic>
#include <string>

struct spa_pod;
struct spa_ringbuffer;

namespace NSROOT
{

// PCM rings (powers of two).
// Own-capture ring: 256KB ~= 1.3s at S16LE 48k stereo.
// Virtual-sink tap rings: 64KB ~= 0.33s each; up to 3 concurrent taps.
#define PW_RING_SIZE         262144
#define PW_TAP_RING_SIZE     65536
#define PW_MAX_TAPS          3
// Drain bite matches upstream PASource FRAME_BUFFER (256 frames).
#define PW_DRAIN_FRAMES      256

class PipeWireLoop;
class PipeWireDrain;
struct PipeWireRuntime;
struct PipeWireTap;

void on_process(void* userdata);
void on_param_changed(void* userdata, uint32_t id, const struct spa_pod* param);

class PipeWireSource : public AudioSource
{
  friend class PipeWireLoop;
  friend class PipeWireDrain;
  friend void on_process(void* userdata);
  friend void on_param_changed(void* userdata, uint32_t id, const struct spa_pod* param);
public:
  // Own-capture mode: connects to target (or default source/monitor).
  PipeWireSource(const std::string& name, const std::string& target);
  // Attach mode: drains one tap of a running virtual sink (no loop).
  PipeWireSource(struct PipeWireRuntime* sharedRt, int tapIdx,
                 const AudioFormat& format);
  virtual ~PipeWireSource() override;

  std::string getName() const override { return m_name; }
  std::string getDescription() const override { return m_target; }
  AudioFormat getFormat() const override { return m_format; }

  void play(OutputStream* out) override;
  void stop() override;

  // true if native PipeWire runtime is reachable (used for fallback logic)
  static bool IsAvailable();

private:
  std::string m_name;
  std::string m_target;
  AudioFormat m_format;
  OutputStream* m_output;

  void(*m_blankKiller)(void*, int, int);

  bool m_ownsRt;
  int m_tapIdx; // attach mode tap, -1 when none
  PipeWireLoop* m_p;
  PipeWireDrain* m_drain;
  PipeWireRuntime* m_rt;
};

// Persistent virtual sink: appears in the OS sound settings as a regular
// output device (e.g. "Sonos"). Apps playing to it feed Sonos-bound taps;
// with no active playback the audio is dropped (apps never stall).
class PipeWireVirtualSink
{
public:
  explicit PipeWireVirtualSink(const std::string& nodeName,
                               const std::string& description);
  ~PipeWireVirtualSink();

  bool start();
  void stop();
  bool isRunning() const;
  bool hasFreeTap() const;
  int attachTap();
  struct PipeWireRuntime* runtime() const { return m_rt; }
  AudioFormat format() const { return m_format; }

private:
  std::string m_name;
  std::string m_desc;
  AudioFormat m_format;
  struct PipeWireRuntime* m_rt;
  class PipeWireLoop* m_loop;
};

}

#endif /* PIPESOURCE_H */
