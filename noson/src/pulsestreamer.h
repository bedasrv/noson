/*
 *      Copyright (C) 2018-2026 Jean-Luc Barriere
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#ifndef PULSESTREAMER_H
#define PULSESTREAMER_H

#include "requestbroker.h"
#include "locked.h"

#include <functional>

#define PULSESTREAMER_CNAME   "pulse"
#define PULSESTREAMER_URI     "/music/pulse.flac"

namespace NSROOT
{

class PipeWireVirtualSink;

class PulseStreamer : public RequestBroker
{
public:
  PulseStreamer(RequestBroker * imageService = nullptr);
  ~PulseStreamer() override;
  virtual bool Initialize() override;
  virtual bool HandleRequest(handle * handle) override;
  // Forward Plasma-slider changes to the Sonos speaker(s). Stashed until
  // the virtual sink exists. Signature matches
  // PipeWireVirtualSink::VolumeHandler (volume01 linear 1.0==100%, mute).
  typedef std::function<void(float volume01, bool mute)> VolumeHandler;
  void SetVolumeHandler(VolumeHandler h);

  const char * CommonName() override { return PULSESTREAMER_CNAME; }
  RequestBroker::ResourcePtr GetResource(const std::string& title) override;
  RequestBroker::ResourceList GetResourceList() override;
  RequestBroker::ResourcePtr RegisterResource(const std::string& title,
                                              const std::string& description,
                                              const std::string& path,
                                              StreamReader * delegate) override;
  void UnregisterResource(const std::string& uri) override;

private:
  ResourceList m_resources;

  // store current index of the pa sink
  Locked<unsigned> m_sinkIndex;
  // count current running playback
  Locked<int> m_playbackCount;
  // persistent PipeWire virtual sink ("Sonos" output device), if enabled
  PipeWireVirtualSink* m_pwSink;
  // stashed slider-to-speaker handler, applied when the sink is created
  VolumeHandler m_volHandler;

  std::string GetPASink();
  void FreePASink();
  void streamSink(handle * handle);
  void streamSinkPW(handle * handle);
  bool UsePipeWire();
  bool EnsureVirtualSink();
};

}

#endif /* PULSESTREAMER_H */

