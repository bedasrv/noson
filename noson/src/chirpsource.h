/*
 *      Copyright (C) 2026 Jean-Luc Barriere / bedasrv
 *
 *  In-process test-tone source for latency measurement.
 *  Generates a periodic logarithmic chirp (no audio hardware involved):
 * -induced signal path is drain -> FLAC -> HTTP -> Sonos, so chirp
 *  send timestamps in the log can be correlated with what is heard
 *  from the Sonos speaker to measure end-to-end latency.
 *  Selected with NOSON_TEST_TONE=chirp.
 */

#ifndef CHIRPSOURCE_H
#define CHIRPSOURCE_H

#include "local_config.h"
#include "audiosource.h"

#include <string>

namespace NSROOT
{

class ChirpWorker;

class ChirpSource : public AudioSource
{
  friend class ChirpWorker;
public:
  ChirpSource(const std::string& name);
  virtual ~ChirpSource() override;

  std::string getName() const override { return m_name; }
  std::string getDescription() const override { return "chirp test tone"; }
  AudioFormat getFormat() const override { return m_format; }

  void play(OutputStream* out) override;
  void stop() override;

private:
  std::string m_name;
  AudioFormat m_format;
  OutputStream* m_output;

  ChirpWorker* m_p;
};

}

#endif /* CHIRPSOURCE_H */
