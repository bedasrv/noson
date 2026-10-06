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

#include "pulsestreamer.h"
#include "local_config.h"
#if defined(HAVE_PULSEAUDIO) || defined(HAVE_PIPEWIRE)
#include "chirpsource.h"
#endif
#ifdef HAVE_PULSEAUDIO
#include "pacontrol.h"
#include "pasource.h"
#endif
#ifdef HAVE_PIPEWIRE
#include "pipesource.h"
#endif
#include "flacencoder.h"
#include "requestbroker.h"
#include "data/datareader.h"
#include "private/debug.h"
#include "private/wsstatic.h"
#include "private/wsrequestbroker.h"
#include "private/wsrequestreply.h"
#include "private/os/threads/timeout.h"

#include <cstring>
#include <cstdlib>
#include <ctime>

/* Important: It MUST match with the static declaration from datareader.cpp */
#define PULSESTREAMER_ICON      "pulseaudio.png"
#define PULSESTREAMER_CONTENT   "audio/flac"
#define PULSESTREAMER_DESC      "Audio stream from %s"
#define PULSESTREAMER_TIMEOUT   10000
#define PULSESTREAMER_MAX_PB    3
#define PULSESTREAMER_CHUNK     32752
#define PULSESTREAMER_TM_MUTE   3000
#define PULSESTREAMER_TM_MUTE_PW 150
#define PULSESTREAMER_CHUNK_PW  8192
#define PA_SINK_NAME            "noson"
#define PA_CLIENT_NAME          PA_SINK_NAME

#ifndef PA_INVALID_INDEX
#define PA_INVALID_INDEX 0xffffffffu
#endif

static unsigned GetMuteTimeoutMs(bool pw)
{
  const char* e = std::getenv("NOSON_MUTE_MS");
  if (e && *e)
  {
    int v = atoi(e);
    if (v >= 0 && v <= 10000)
      return (unsigned)v;
  }
  return pw ? PULSESTREAMER_TM_MUTE_PW : PULSESTREAMER_TM_MUTE;
}

static unsigned GetChunkBytes()
{
  const char* e = std::getenv("NOSON_CHUNK_BYTES");
  if (e && *e)
  {
    int v = atoi(e);
    if (v >= 4096 && v <= PULSESTREAMER_CHUNK)
      return (unsigned)v;
  }
  return PULSESTREAMER_CHUNK_PW;
}

static int64_t mono_ms()
{
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

using namespace NSROOT;

PulseStreamer::PulseStreamer(RequestBroker * imageService /*= nullptr*/)
: RequestBroker()
, m_resources()
, m_sinkIndex(PA_INVALID_INDEX)
, m_playbackCount(0)
, m_pwSink(nullptr)
{
  // delegate image download to imageService
  ResourcePtr img(nullptr);
  if (imageService)
    img = imageService->RegisterResource(PULSESTREAMER_CNAME,
                                         "Icon for " PULSESTREAMER_CNAME,
                                         PULSESTREAMER_ICON,
                                         DataReader::Instance());

  // declare the static resource
  ResourcePtr ptr = ResourcePtr(new Resource());
  ptr->uri = PULSESTREAMER_URI;
  ptr->title = PULSESTREAMER_CNAME;
  ptr->description = PULSESTREAMER_DESC;
  ptr->contentType = PULSESTREAMER_CONTENT;
  if (img)
    ptr->iconUri.assign(img->uri).append("?id=" LIBVERSION);
  m_resources.push_back(ptr);
}

bool PulseStreamer::Initialize()
{
#ifdef HAVE_PIPEWIRE
  // Best-effort: Sonos output device visible in sound settings from app
  // start. A missing/broken PipeWire never fails init; streaming falls
  // back to other paths.
  EnsureVirtualSink();
#endif
#ifdef HAVE_PULSEAUDIO
  if (initialize_pulse(1) == 0)
    return true;
#endif
#ifdef HAVE_PIPEWIRE
  return true;
#else
  return false;
#endif
}

PulseStreamer::~PulseStreamer()
{
#ifdef HAVE_PIPEWIRE
  delete m_pwSink;
  m_pwSink = nullptr;
#endif
}

// Persistent virtual sink ("Sonos" output device in sound settings).
// Created on first stream so a missing/broken PipeWire never blocks init.
bool PulseStreamer::EnsureVirtualSink()
{
#ifdef HAVE_PIPEWIRE
  if (m_pwSink)
    return m_pwSink->isRunning();
  const char* dis = std::getenv("NOSON_VIRTUAL_SINK");
  if (dis && strcmp(dis, "0") == 0)
    return false;
  if (!PipeWireSource::IsAvailable())
    return false;
  const char* nm = std::getenv("NOSON_SINK_NAME");
  const char* ds = std::getenv("NOSON_SINK_DESC");
  m_pwSink = new PipeWireVirtualSink(nm ? nm : "", ds ? ds : "");
  if (!m_pwSink->start())
  {
    DBG(DBG_ERROR, "%s: virtual sink unavailable\n", __FUNCTION__);
    delete m_pwSink;
    m_pwSink = nullptr;
    return false;
  }
  return true;
#else
  return false;
#endif
}

bool PulseStreamer::HandleRequest(handle * handle)
{
  if (!IsAborted())
  {
    const std::string& requrl = handle->broker->GetRequestPath();
    if (requrl.compare(0, strlen(PULSESTREAMER_URI), PULSESTREAMER_URI) == 0)
    {
      switch (handle->broker->GetRequestMethod())
      {
      case WS_METHOD_Get:
        streamSink(handle);
        return true;
      case WS_METHOD_Head:
      {
        TraceResponseStatus(200);
        WSRequestReply reply(*handle->broker);
        reply.AddHeader(WS_HEADER_Content_Type, "audio/flac");
        reply.AddHeader(WS_HEADER_Accept_Ranges, "none");
        reply.CloseReply(WS_STATUS_200_OK);
        return true;
      }
      default:
        return false; // unhandled method
      }
    }
  }
  return false;
}

RequestBroker::ResourcePtr PulseStreamer::GetResource(const std::string& title)
{
  (void)title;
  return m_resources.front();
}

RequestBroker::ResourceList PulseStreamer::GetResourceList()
{
  ResourceList list;
  for (ResourceList::iterator it = m_resources.begin(); it != m_resources.end(); ++it)
    list.push_back((*it));
  return list;
}

RequestBroker::ResourcePtr PulseStreamer::RegisterResource(const std::string& title,
                                                           const std::string& description,
                                                           const std::string& path,
                                                           StreamReader * delegate)
{
  (void)title;
  (void)description;
  (void)path;
  (void)delegate;
  return ResourcePtr();
}

void PulseStreamer::UnregisterResource(const std::string& uri)
{
  (void)uri;
}

bool PulseStreamer::UsePipeWire()
{
#ifdef HAVE_PIPEWIRE
  const char* backend = std::getenv("NOSON_AUDIO_BACKEND");
  if (backend && *backend)
  {
    if (strcmp(backend, "pulse") == 0)
      return false;
    if (strcmp(backend, "pipewire") == 0)
      return true;
  }
  // Prefer native PipeWire when the daemon is reachable; fallback to Pulse.
  if (PipeWireSource::IsAvailable())
    return true;
#endif
  return false;
}

std::string PulseStreamer::GetPASink()
{
#ifdef HAVE_PULSEAUDIO
  std::string deviceName;
  PAControl::SinkList sinks;
  PAControl pacontrol(PA_CLIENT_NAME);

  bool cont = true;
  for (;;)
  {
    // get sink list
    if (pacontrol.connect())
    {
      pacontrol.getSinkList(&sinks);
      pacontrol.disconnect();
    }
    else
    {
      DBG(DBG_ERROR, "%s: failed to connect to pulse\n", __FUNCTION__);
      break;
    }
    // search the sink in list
    for (PAControl::Sink& ad : sinks)
    {
      if (ad.name == PA_SINK_NAME)
      {
        DBG(DBG_DEBUG, "%s: found device %u: %s (%u)\n", __FUNCTION__, ad.index, ad.monitorSourceName.c_str(), ad.ownerModule);
        deviceName = ad.monitorSourceName;
        m_sinkIndex.Store(ad.ownerModule); // own the module
        break;
      }
    }
    if (!deviceName.empty() || !cont)
      break;
    // no sink exist so create it
    DBG(DBG_DEBUG, "%s: create sink (%s)\n", __FUNCTION__, PA_SINK_NAME);
    if (pacontrol.connect())
    {
      m_sinkIndex.Store(pacontrol.newSink(PA_SINK_NAME, PA_SINK_NAME));
      pacontrol.disconnect();
      cont = false;
    }
    else
      break;
  }
  return deviceName;
#else
  return std::string();
#endif
}

void PulseStreamer::FreePASink()
{
#ifdef HAVE_PULSEAUDIO
  PAControl pacontrol(PA_CLIENT_NAME);
  if (pacontrol.connect())
  {
    DBG(DBG_DEBUG, "%s: delete sink (%s)\n", __FUNCTION__, PA_SINK_NAME);
    pacontrol.deleteSink(m_sinkIndex.Load());
    pacontrol.disconnect();
  }
#endif
}

void PulseStreamer::streamSink(handle * handle)
{
  if (UsePipeWire())
  {
    streamSinkPW(handle);
    return;
  }
#ifdef HAVE_PULSEAUDIO
  WSRequestReply reply(*handle->broker);
  if (!handle->broker->GetRequestHeader(WS_HEADER_Range).empty())
  {
    DBG(DBG_WARN, "%s: cannot seek in stream\n", __FUNCTION__);
    TraceResponseStatus(400);
    reply.CloseReply(WS_STATUS_400_Bad_Request);
    return;
  }

  *m_playbackCount.GetExclusive() += 1;
  std::string deviceName = GetPASink();

  if (deviceName.empty())
  {
    DBG(DBG_WARN, "%s: no sink available\n", __FUNCTION__);
    TraceResponseStatus(503);
    reply.CloseReply(WS_STATUS_503_Service_Unavailable);
  }
  else if (m_playbackCount.Load() > PULSESTREAMER_MAX_PB)
  {
    TraceResponseStatus(429);
    reply.CloseReply(WS_STATUS_429_Too_Many_Requests);
  }
  else
  {
    PASource audioSource(PA_CLIENT_NAME, deviceName);
    FLACEncoder audioEncoder;
    BufferedStream stream(64);
    audioEncoder.open(audioSource.getFormat(), &stream);

    // the source is muted for a short time to limit output rate on startup
    audioSource.mute(true);
    OS::Timeout muted(GetMuteTimeoutMs(false));

    audioSource.play(&audioEncoder);

    TraceResponseStatus(200);
    reply.AddHeader(WS_HEADER_Content_Type, "audio/flac");
    reply.AddHeader(WS_HEADER_Accept_Ranges, "none");
    reply.AddHeader(WS_HEADER_Transfer_Encoding, "chunked");
    if (reply.PostReply(WS_STATUS_200_OK))
    {
      char * buf = new char [PULSESTREAMER_CHUNK + 16];
      int r = 0;
      while (!IsAborted() && (r = stream.ReadAsync(buf + 5 + WS_CRLF_LEN, PULSESTREAMER_CHUNK, PULSESTREAMER_TIMEOUT)) > 0)
      {
        char str[5 + WS_CRLF_LEN + 1];
        snprintf(str, sizeof(str), "%05x" WS_CRLF, (unsigned)r & 0xfffff);
        memcpy(buf, str, 5 + WS_CRLF_LEN);
        memcpy(buf + 5 + WS_CRLF_LEN + r, WS_CRLF, WS_CRLF_LEN);
        if (!handle->broker->ReplyData(buf, 5 + WS_CRLF_LEN + r + WS_CRLF_LEN))
          break;
        // disable source mute after delay
        if (audioSource.muted() && !muted.time_left())
          audioSource.mute(false);
      }
      delete [] buf;
      if (r == 0)
        handle->broker->ReplyData("0" WS_CRLF WS_CRLF, 1 + WS_CRLF_LEN + WS_CRLF_LEN);
    }

    audioSource.stop();
    audioEncoder.close();
  }

  *m_playbackCount.GetExclusive() -= 1;
  // Check an other playback is running before delete the sink
  Locked<int>::pointer p = m_playbackCount.GetExclusive();
  if (*p == 0)
    FreePASink();
#else
  WSRequestReply reply(*handle->broker);
  TraceResponseStatus(503);
  reply.CloseReply(WS_STATUS_503_Service_Unavailable);
#endif
}

void PulseStreamer::streamSinkPW(handle * handle)
{
#ifdef HAVE_PIPEWIRE
  WSRequestReply reply(*handle->broker);
  if (!handle->broker->GetRequestHeader(WS_HEADER_Range).empty())
  {
    DBG(DBG_WARN, "%s: cannot seek in stream\n", __FUNCTION__);
    TraceResponseStatus(400);
    reply.CloseReply(WS_STATUS_400_Bad_Request);
    return;
  }

  *m_playbackCount.GetExclusive() += 1;
  if (m_playbackCount.Load() > PULSESTREAMER_MAX_PB)
  {
    TraceResponseStatus(429);
    reply.CloseReply(WS_STATUS_429_Too_Many_Requests);
    *m_playbackCount.GetExclusive() -= 1;
    return;
  }

  // Target selection: explicit NOSON_PW_TARGET wins (e.g. raw mic);
  // otherwise the persistent virtual sink ("Sonos" output device);
  // otherwise the default sink monitor. No PulseAudio null-sink is
  // created on this path. NOSON_TEST_TONE=chirp sends an in-process
  // test chirp instead of audio hardware, with per-chirp send
  // timestamps for latency measurement.
  const char* envTarget = std::getenv("NOSON_PW_TARGET");
  std::string target = envTarget ? envTarget : "";
  const char* testTone = std::getenv("NOSON_TEST_TONE");
  const bool useChirp = testTone && strcmp(testTone, "chirp") == 0;
  const bool useSink = !useChirp && target.empty() && EnsureVirtualSink()
      && m_pwSink->hasFreeTap();
  DBG(DBG_INFO, "%s: %s target='%s'\n", __FUNCTION__,
      useChirp ? "chirp test tone" : (useSink ? "virtual sink" : "pipewire capture"),
      useChirp ? "<internal>" : (target.empty() && useSink ? "<sonos sink>" :
        (target.empty() ? "<default sink monitor>" : target.c_str())));

  AudioSource* audioSource = nullptr;
  PipeWireSource* pwSource = nullptr;
  ChirpSource* chirpSource = nullptr;
  if (useChirp)
  {
    chirpSource = new ChirpSource(PA_CLIENT_NAME);
    audioSource = chirpSource;
  }
  else if (useSink)
  {
    int tap = m_pwSink->attachTap();
    if (tap < 0)
    {
      DBG(DBG_ERROR, "%s: no free virtual-sink tap\n", __FUNCTION__);
      TraceResponseStatus(503);
      reply.CloseReply(WS_STATUS_503_Service_Unavailable);
      *m_playbackCount.GetExclusive() -= 1;
      return;
    }
    pwSource = new PipeWireSource(m_pwSink->runtime(), tap,
                                  m_pwSink->format());
    audioSource = pwSource;
  }
  else
  {
    pwSource = new PipeWireSource(PA_CLIENT_NAME, target);
    audioSource = pwSource;
  }
  FLACEncoder audioEncoder;
  BufferedStream stream(64);
  if (!audioEncoder.open(audioSource->getFormat(), &stream))
  {
    DBG(DBG_ERROR, "%s: flac open failed\n", __FUNCTION__);
    TraceResponseStatus(500);
    reply.CloseReply(WS_STATUS_500_Internal_Server_Error);
    *m_playbackCount.GetExclusive() -= 1;
    delete audioSource;
    return;
  }

  audioSource->mute(true);
  OS::Timeout muted(GetMuteTimeoutMs(true));
  audioSource->play(&audioEncoder);

  const unsigned chunkBytes = GetChunkBytes();
  int64_t tStart = mono_ms();
  uint64_t totalBytes = 0;
  int64_t tNextLog = tStart + 2000;

  TraceResponseStatus(200);
  reply.AddHeader(WS_HEADER_Content_Type, "audio/flac");
  reply.AddHeader(WS_HEADER_Accept_Ranges, "none");
  reply.AddHeader(WS_HEADER_Transfer_Encoding, "chunked");
  if (reply.PostReply(WS_STATUS_200_OK))
  {
    char * buf = new char [chunkBytes + 16];
    int r = 0;
    while (!IsAborted() && (r = stream.ReadAsync(buf + 5 + WS_CRLF_LEN, chunkBytes, PULSESTREAMER_TIMEOUT)) > 0)
    {
      char str[5 + WS_CRLF_LEN + 1];
      snprintf(str, sizeof(str), "%05x" WS_CRLF, (unsigned)r & 0xfffff);
      memcpy(buf, str, 5 + WS_CRLF_LEN);
      memcpy(buf + 5 + WS_CRLF_LEN + r, WS_CRLF, WS_CRLF_LEN);
      if (!handle->broker->ReplyData(buf, 5 + WS_CRLF_LEN + r + WS_CRLF_LEN))
        break;
      totalBytes += (uint64_t)r;
      int64_t now = mono_ms();
      if (now >= tNextLog)
      {
        double secs = (now - tStart) / 1000.0;
        DBG(DBG_INFO, "%s: sent %llu bytes in %.1fs (%.1f kB/s)\n", __FUNCTION__,
            (unsigned long long)totalBytes, secs,
            secs > 0 ? totalBytes / 1024.0 / secs : 0.0);
        tNextLog = now + 2000;
      }
      if (audioSource->muted() && !muted.time_left())
        audioSource->mute(false);
    }
    delete [] buf;
    if (r == 0)
      handle->broker->ReplyData("0" WS_CRLF WS_CRLF, 1 + WS_CRLF_LEN + WS_CRLF_LEN);
  }

  audioSource->stop();
  audioEncoder.close();
  delete audioSource;
  *m_playbackCount.GetExclusive() -= 1;
  // No null-sink to free on the PipeWire path
#else
  (void)handle;
#endif
}
