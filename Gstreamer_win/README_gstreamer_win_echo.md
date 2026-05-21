# GStreamer Windows Timestamp Echo

Two-machine defaults:

- Server camera/source machine: `10.78.62.71`
- Client display/echo machine: `10.78.62.148`
- Video: UDP `5004`
- RTT echo: TCP `5005`

## Prerequisites

Install the official GStreamer Python wheel bundle on both Windows machines:

```powershell
python -m pip install gstreamer-bundle
```

Verify on both machines:

```powershell
gst-inspect-1.0 x264enc
gst-inspect-1.0 rtph264pay
python -c "import gi; gi.require_version('Gst','1.0'); from gi.repository import Gst; Gst.init(None); print(Gst.version_string())"
```

Verify on the server machine:

```powershell
gst-inspect-1.0 ksvideosrc
```

If `ksvideosrc` is unavailable or does not see the camera, the server script
also tries `dshowvideosrc`, `mfvideosrc`, and `autovideosrc`.

## Firewall

Allow inbound traffic:

- Client machine `10.78.62.148`: UDP `5004`
- Server machine `10.78.62.71`: TCP `5005`

## Run

Edit shared settings in:

```powershell
.\Gstreamer_win\gst_win_echo_config.json
```

Important sections:

- `network`: server/client IP addresses and UDP/TCP ports
- `video`: camera index, source element, resolution, FPS, and input caps
- `gstreamer`: payload type, encoder bitrate/keyframe settings, queue/sink settings, and RTT averaging

Start the server on `10.78.62.71`:

```powershell
python .\Gstreamer_win\gst_win_server_echo.py
```

Start the client on `10.78.62.148`:

```powershell
python .\Gstreamer_win\gst_win_client_echo.py
```

To use another config file:

```powershell
python .\Gstreamer_win\gst_win_server_echo.py --config .\my_config.json
python .\Gstreamer_win\gst_win_client_echo.py --config .\my_config.json
```

The server prints RTT lines when the client receives RTP packets and echoes the
embedded sequence/timestamp back.

## Useful Options

Server defaults:

```powershell
python .\Gstreamer_win\gst_win_server_echo.py --client-ip 10.78.62.148 --udp-port 5004 --echo-port 5005 --device-index 0 --width 1920 --height 1080 --fps 30 --bitrate 30000
```

Client defaults:

```powershell
python .\Gstreamer_win\gst_win_client_echo.py --server-ip 10.78.62.71 --udp-port 5004 --echo-port 5005 --jitter-latency 0
```

If the camera does not support MJPEG 1080p30, try a lower resolution:

```powershell
python .\Gstreamer_win\gst_win_server_echo.py --width 1280 --height 720 --fps 30
```

If the camera outputs raw video instead of MJPEG:

```powershell
python .\Gstreamer_win\gst_win_server_echo.py --input-format video/x-raw --width 1280 --height 720 --fps 30
```

For a complete custom source caps string:

```powershell
python .\Gstreamer_win\gst_win_server_echo.py --source-caps "image/jpeg,width=1280,height=720,framerate=30/1"
```
