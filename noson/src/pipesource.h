/*
 *      Copyright (C) 2026 Jean-Luc Barriere / bedasrv
 *
 *  PipeWire native low-latency capture source.
 *  Replaces the legacy pa_simple blocking path (PASource) when available.
 *  Target object can be forced via NOSON_PW_TARGET (node name or id),
 *  e.g. alsa_input.pci-0000_00_1f.3.analog-stereo for raw mic tests.
 *  Latency via NOSON_PW_LATENCY (default "256/48000").
 */

#ifndef PIPESOURCE_H
#define PIPESOURCE_H

#include "local_config.h"
#include "audiosource.h"

#include <string>

struct spa_pod;

namespace NSROOT
{

class PipeWireLoop;
class PipeWireDrain;
struct PipeWireRuntime;

void on_process(void* userdata);
void on_param_changed(void* userdata, uint32_t id, const struct spa_pod* param);

class PipeWireSource : public AudioSource
{
  friend class PipeWireLoop;
  friend class PipeWireDrain;
  friend void on_process(void* userdata);
  friend void on_param_changed(void* userdata, uint32_t id, const struct spa_pod* param);
public:
  PipeWireSource(const std::string& name, const std::string& target);
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

  PipeWireLoop* m_p;
  PipeWireDrain* m_drain;
  PipeWireRuntime* m_rt;
};

}

#endif /* PIPESOURCE_H */
