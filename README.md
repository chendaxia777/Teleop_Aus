# Teleop_Aus Quick Wiki

This repo contains two-machine networking examples using ZeroTier, Zenoh, and GStreamer.

## 1. Install dependencies

Clone the repo on both machines:

```bash
git clone https://github.com/chendaxia777/Teleop_Aus.git
cd Teleop_Aus
```

Install the required Python packages on both machines:

```bash
pip install eclipse-zenoh
pip install gstreamer-bundle
```

## 2. ZeroTier setup

1. Download and install ZeroTier: https://www.zerotier.com/download/
2. Join the ZeroTier network on both machines using the provided Network ID.
3. Check the ZeroTier managed IP address for each machine.

## 3. Zenoh setup

Machine A/B runs the Zenoh router and server-side Zenoh script. Machine B connects to Machine A through the ZeroTier IP.

### Machine A/B: start Zenoh router

Open a terminal on Machine A:

```bash
zenohd -l tcp/0.0.0.0:7447
```

Keep this router running.

### Configure Zenoh endpoint

For the keyboard Zenoh scripts, edit this file on both machines:

```text
trial/z_server_config.json
```

Set `router_ip_address` to Router's ZeroTier IP:

```json
{
  "protocol": "tcp",
  "router_ip_address": "<MACHINE_ROUTER_ZEROTIER_IP>",
  "port": 7447
}
```

### Run Zenoh scripts

On Machine A, run the server-side script:

```bash
python ./trial/z_server_keyboard.py
```

On Machine B, run the client-side script:

```bash
python ./trial/z_client_keyboard.py
```

## 4. GStreamer setup

GStreamer uses one machine as the camera/source server and the other as the client/display machine.

Edit the shared config file:

```text
Gstreamer_win/gst_win_echo_config.json
```

Update the IP addresses:

```json
{
  "network": {
    "server_ip": "<MACHINE_A_ZEROTIER_IP>",
    "client_ip": "<MACHINE_B_ZEROTIER_IP>",
    "udp_port": 5004,
    "echo_port": 5005
  }
}
```

Run the server script on Machine A:

```bash
python ./Gstreamer_win/gst_win_server_echo.py
```

Run the client script on Machine B:

```bash
python ./Gstreamer_win/gst_win_client_echo.py
```

## 5. Quick checks

If the GStreamer server cannot find the camera, try lowering resolution/FPS or changing the video source settings in `Gstreamer_win/gst_win_echo_config.json`.
