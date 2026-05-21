#!/usr/bin/env python3
"""
Windows GStreamer video receiver with RTP timestamp echo.

Client machine:
  IP: 10.78.62.148

This script receives RTP/H.264 video from the server, displays it, extracts the
server timestamp from each RTP packet header extension, and echoes the sequence
and timestamp back to the server over TCP.
"""

from __future__ import annotations

import argparse
import signal
import socket
import struct
import sys
import threading
import time

try:
    import gi

    gi.require_version("Gst", "1.0")
    gi.require_version("GstRtp", "1.0")
    from gi.repository import Gst, GLib, GstRtp
except (ImportError, ValueError) as exc:
    print("Failed to import GStreamer Python bindings.")
    print("Install GStreamer MSVC runtime/development packages and PyGObject.")
    print(f"Details: {exc}")
    sys.exit(1)


DEFAULT_SERVER_IP = "10.78.62.71"
DEFAULT_UDP_PORT = 5004
DEFAULT_ECHO_PORT = 5005
RTP_EXTENSION_ID = 1
ECHO_STRUCT = struct.Struct("!HQ")


class WindowsTimestampEchoClient:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.pipeline = None
        self.loop = None

        self.tcp_client = None
        self.tcp_connected = False
        self.tcp_lock = threading.Lock()
        self.stop_lock = threading.Lock()
        self.stopping = False

        self.received_packets = 0
        self.echoed_packets = 0
        self.missing_extension_packets = 0
        self.last_missing_log_ns = 0

    def _require_elements(self, element_names: list[str]) -> bool:
        missing = [name for name in element_names if Gst.ElementFactory.find(name) is None]
        if not missing:
            return True

        print("Missing required GStreamer element(s): " + ", ".join(missing))
        print("Install GStreamer Base, Good, Bad, Ugly, and Libav plugin packages.")
        return False

    def create_pipeline(self) -> bool:
        Gst.init(None)

        required = [
            "udpsrc",
            "rtpjitterbuffer",
            "rtph264depay",
            "h264parse",
            "avdec_h264",
            "videoconvert",
            "autovideosink",
        ]
        if not self._require_elements(required):
            return False

        self.pipeline = Gst.Pipeline.new("win-timestamp-echo-client")

        udpsrc = Gst.ElementFactory.make("udpsrc", "udpsrc")
        udpsrc.set_property("port", self.args.udp_port)
        caps = Gst.Caps.from_string(
            f"application/x-rtp,media=video,encoding-name=H264,payload={self.args.payload_type}"
        )
        udpsrc.set_property("caps", caps)

        jitterbuffer = Gst.ElementFactory.make("rtpjitterbuffer", "jitterbuffer")
        jitterbuffer.set_property("latency", self.args.jitter_latency)

        depay = Gst.ElementFactory.make("rtph264depay", "depay")
        h264parse = Gst.ElementFactory.make("h264parse", "h264parse")
        decoder = Gst.ElementFactory.make("avdec_h264", "decoder")
        videoconvert = Gst.ElementFactory.make("videoconvert", "videoconvert")
        videosink = Gst.ElementFactory.make("autovideosink", "videosink")
        videosink.set_property("sync", False)

        elements = [udpsrc, jitterbuffer, depay, h264parse, decoder, videoconvert, videosink]
        for element in elements:
            if element is None:
                print("Could not create one or more GStreamer pipeline elements.")
                return False
            self.pipeline.add(element)

        if not udpsrc.link(jitterbuffer):
            print("Could not link udpsrc to rtpjitterbuffer.")
            return False
        if not jitterbuffer.link(depay):
            print("Could not link rtpjitterbuffer to rtph264depay.")
            return False
        if not depay.link(h264parse):
            print("Could not link rtph264depay to h264parse.")
            return False
        if not h264parse.link(decoder):
            print("Could not link h264parse to avdec_h264.")
            return False
        if not decoder.link(videoconvert):
            print("Could not link avdec_h264 to videoconvert.")
            return False
        if not videoconvert.link(videosink):
            print("Could not link videoconvert to autovideosink.")
            return False

        pad = jitterbuffer.get_static_pad("src")
        if pad is None:
            print("Could not access jitterbuffer src pad for timestamp extraction.")
            return False
        pad.add_probe(Gst.PadProbeType.BUFFER, self._on_rtp_in)
        return True

    def _bytes_from_extension_data(self, data, size: int) -> bytes:
        if hasattr(data, "get_data"):
            raw = data.get_data()
        elif isinstance(data, (bytes, bytearray, memoryview)):
            raw = bytes(data)
        else:
            raw = bytes(data)
        return raw[:size] if size is not None else raw

    def _get_extension(self, rtp) -> bytes | None:
        attempts = (
            lambda: rtp.get_extension_onebyte_header(RTP_EXTENSION_ID, 0),
            lambda: rtp.get_extension_onebyte_header(RTP_EXTENSION_ID),
        )
        for attempt in attempts:
            try:
                result = attempt()
            except TypeError:
                continue

            if not isinstance(result, tuple) or not result:
                continue

            found = bool(result[0])
            if not found or len(result) < 2:
                return None

            data = result[1]
            size = result[2] if len(result) > 2 else len(data)
            return self._bytes_from_extension_data(data, size)

        return None

    def _on_rtp_in(self, pad, info):
        buffer = info.get_buffer()
        if buffer is None:
            return Gst.PadProbeReturn.OK

        success, rtp = GstRtp.RTPBuffer.map(buffer, Gst.MapFlags.READ)
        if not success:
            return Gst.PadProbeReturn.OK

        try:
            self.received_packets += 1
            extension = self._get_extension(rtp)
            if extension is None or len(extension) < ECHO_STRUCT.size:
                self._log_missing_extension()
                return Gst.PadProbeReturn.OK

            seq, timestamp_ns = ECHO_STRUCT.unpack(extension[: ECHO_STRUCT.size])
            self._send_echo(seq, timestamp_ns)
        finally:
            rtp.unmap()

        return Gst.PadProbeReturn.OK

    def _log_missing_extension(self):
        self.missing_extension_packets += 1
        now_ns = time.time_ns()
        if now_ns - self.last_missing_log_ns >= 2_000_000_000:
            print(
                "Waiting for RTP timestamp extensions "
                f"(missing so far: {self.missing_extension_packets})"
            )
            self.last_missing_log_ns = now_ns

    def _connect_tcp_locked(self) -> bool:
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(1.0)
            client.connect((self.args.server_ip, self.args.echo_port))
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            client.setblocking(False)
            self.tcp_client = client
            self.tcp_connected = True
            print(f"Connected to RTT echo server at {self.args.server_ip}:{self.args.echo_port}")
            return True
        except OSError:
            self.tcp_client = None
            self.tcp_connected = False
            return False

    def _send_echo(self, seq: int, timestamp_ns: int):
        payload = ECHO_STRUCT.pack(seq, timestamp_ns)
        with self.tcp_lock:
            if not self.tcp_connected or self.tcp_client is None:
                if not self._connect_tcp_locked():
                    return

            try:
                self.tcp_client.sendall(payload)
                self.echoed_packets += 1
            except OSError:
                self._close_tcp_locked()

    def _close_tcp_locked(self):
        if self.tcp_client is not None:
            try:
                self.tcp_client.close()
            except OSError:
                pass
        self.tcp_client = None
        self.tcp_connected = False

    def close_tcp(self):
        with self.tcp_lock:
            self._close_tcp_locked()

    def on_message(self, bus, message):
        msg_type = message.type
        if msg_type == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            print(f"GStreamer error: {err.message}")
            if debug:
                print(f"Debug info: {debug}")
            self.stop()
        elif msg_type == Gst.MessageType.WARNING:
            warn, debug = message.parse_warning()
            print(f"GStreamer warning: {warn.message}")
            if debug:
                print(f"Debug info: {debug}")
        elif msg_type == Gst.MessageType.EOS:
            print("End of stream.")
            self.stop()
        elif msg_type == Gst.MessageType.STATE_CHANGED and message.src == self.pipeline:
            old_state, new_state, _pending = message.parse_state_changed()
            print(f"Pipeline state changed: {old_state.value_nick} -> {new_state.value_nick}")
        return True

    def start(self) -> bool:
        if not self.create_pipeline():
            return False

        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self.on_message)

        self.loop = GLib.MainLoop()

        def signal_handler(_sig, _frame):
            print("\nStopping...")
            self.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        print(f"Receiving video on UDP port {self.args.udp_port}")
        print(f"Echoing timestamps to {self.args.server_ip}:{self.args.echo_port}")
        print(f"Jitterbuffer latency: {self.args.jitter_latency} ms")
        print("Press Ctrl+C to stop.")

        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            print("Unable to set receiver pipeline to PLAYING.")
            self.stop()
            return False

        self.loop.run()
        return True

    def stop(self):
        with self.stop_lock:
            if self.stopping:
                return
            self.stopping = True

        self.close_tcp()

        if self.pipeline is not None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None

        print(
            "Final counts: "
            f"rtp_received={self.received_packets}, "
            f"echoes_sent={self.echoed_packets}, "
            f"missing_extensions={self.missing_extension_packets}"
        )

        if self.loop is not None and self.loop.is_running():
            self.loop.quit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Windows GStreamer client: receive video and echo RTP timestamps."
    )
    parser.add_argument("--server-ip", default=DEFAULT_SERVER_IP)
    parser.add_argument("--udp-port", type=int, default=DEFAULT_UDP_PORT)
    parser.add_argument("--echo-port", type=int, default=DEFAULT_ECHO_PORT)
    parser.add_argument("--jitter-latency", type=int, default=0)
    parser.add_argument("--payload-type", type=int, default=96)
    return parser.parse_args()


def main() -> int:
    client = WindowsTimestampEchoClient(parse_args())
    return 0 if client.start() else 1


if __name__ == "__main__":
    sys.exit(main())
