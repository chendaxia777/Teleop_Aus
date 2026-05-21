#!/usr/bin/env python3
"""
Windows GStreamer video sender with RTP timestamp echo RTT measurement.

Server machine:
  IP: 10.78.62.71

This script captures camera video on the server, streams RTP/H.264 video to the
client over UDP, embeds a server timestamp in each RTP packet header extension,
and prints RTT when the client echoes the sequence/timestamp over TCP.
"""

from __future__ import annotations

import argparse
import collections
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


DEFAULT_CLIENT_IP = "10.78.62.148"
DEFAULT_UDP_PORT = 5004
DEFAULT_ECHO_PORT = 5005
RTP_EXTENSION_ID = 1
ECHO_STRUCT = struct.Struct("!HQ")


class WindowsTimestampEchoServer:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.pipeline = None
        self.loop = None

        self.echo_server = None
        self.echo_running = False
        self.echo_thread = None
        self.stop_lock = threading.Lock()
        self.stopping = False

        self.sent_packets = 0
        self.extension_packets = 0
        self.echoed_packets = 0
        self.rtt_window = collections.deque(maxlen=args.avg_window)
        self.last_extension_warning_ns = 0

    def _set_optional_property(self, element, property_name: str, value):
        if value is not None and element.find_property(property_name):
            element.set_property(property_name, value)

    def _make_source(self):
        candidates = [
            self.args.source_element,
            "ksvideosrc",
            "dshowvideosrc",
            "mfvideosrc",
            "autovideosrc",
        ]
        seen = set()

        for name in candidates:
            if not name or name in seen:
                continue
            seen.add(name)

            source = Gst.ElementFactory.make(name, "source")
            if source is None:
                continue

            self._set_optional_property(source, "do-timestamp", True)
            self._set_optional_property(source, "device-index", self.args.device_index)
            self._set_optional_property(source, "device-name", self.args.device_name)
            self._set_optional_property(source, "device-path", self.args.device_path)

            if name == "dshowvideosrc":
                self._set_optional_property(source, "device", self.args.device_path)

            print(f"Using camera source: {name}")
            return source, name

        print("Could not create a Windows camera source.")
        print("Tried: " + ", ".join(dict.fromkeys(candidates)))
        return None, None

    def _source_caps(self) -> str:
        if self.args.source_caps:
            return self.args.source_caps
        return (
            f"{self.args.input_format},"
            f"width={self.args.width},"
            f"height={self.args.height},"
            f"framerate={self.args.fps}/1"
        )

    def _require_elements(self, element_names: list[str]) -> bool:
        missing = [name for name in element_names if Gst.ElementFactory.find(name) is None]
        if not missing:
            return True

        print("Missing required GStreamer element(s): " + ", ".join(missing))
        print("Install GStreamer Base, Good, Bad, Ugly, and Libav plugin packages.")
        return False

    def create_pipeline(self) -> bool:
        Gst.init(None)

        needs_jpegdec = self.args.input_format == "image/jpeg"
        required = ["capsfilter", "videoconvert", "queue", "x264enc", "h264parse", "rtph264pay", "udpsink"]
        if needs_jpegdec:
            required.append("jpegdec")
        if not self._require_elements(required):
            return False

        self.pipeline = Gst.Pipeline.new("win-timestamp-echo-server")
        source, source_name = self._make_source()
        if source is None:
            return False

        caps_filter = Gst.ElementFactory.make("capsfilter", "caps_filter")
        caps_filter.set_property("caps", Gst.Caps.from_string(self._source_caps()))

        jpegdec = Gst.ElementFactory.make("jpegdec", "jpegdec") if needs_jpegdec else None
        videoconvert = Gst.ElementFactory.make("videoconvert", "videoconvert")

        queue = Gst.ElementFactory.make("queue", "queue")
        queue.set_property("max-size-buffers", 1)
        queue.set_property("leaky", 2)

        encoder = Gst.ElementFactory.make("x264enc", "encoder")
        encoder.set_property("tune", 0x00000004)
        encoder.set_property("speed-preset", 1)
        encoder.set_property("bitrate", self.args.bitrate)
        encoder.set_property("key-int-max", self.args.key_int_max)
        encoder.set_property("bframes", 0)

        h264parse = Gst.ElementFactory.make("h264parse", "h264parse")
        h264parse.set_property("config-interval", 1)

        rtppay = Gst.ElementFactory.make("rtph264pay", "rtppay")
        rtppay.set_property("pt", self.args.payload_type)
        rtppay.set_property("config-interval", 1)

        udpsink = Gst.ElementFactory.make("udpsink", "udpsink")
        udpsink.set_property("host", self.args.client_ip)
        udpsink.set_property("port", self.args.udp_port)
        udpsink.set_property("sync", False)
        udpsink.set_property("async", False)

        elements = [source, caps_filter]
        if jpegdec is not None:
            elements.append(jpegdec)
        elements.extend([videoconvert, queue, encoder, h264parse, rtppay, udpsink])

        for element in elements:
            if element is None:
                print("Could not create one or more GStreamer pipeline elements.")
                return False
            self.pipeline.add(element)

        if not source.link(caps_filter):
            print(f"Could not link {source_name} to capsfilter. Check camera caps: {self._source_caps()}")
            return False

        if jpegdec is not None:
            if not caps_filter.link(jpegdec) or not jpegdec.link(videoconvert):
                print("Could not link MJPEG decode branch. Try --input-format video/x-raw if your camera is not MJPEG.")
                return False
        elif not caps_filter.link(videoconvert):
            print("Could not link raw camera caps to videoconvert.")
            return False

        if not videoconvert.link(queue):
            print("Could not link videoconvert to queue.")
            return False
        if not queue.link(encoder):
            print("Could not link queue to x264enc.")
            return False
        if not encoder.link(h264parse):
            print("Could not link x264enc to h264parse.")
            return False
        if not h264parse.link(rtppay):
            print("Could not link h264parse to rtph264pay.")
            return False
        if not rtppay.link(udpsink):
            print("Could not link rtph264pay to udpsink.")
            return False

        pad = rtppay.get_static_pad("src")
        if pad is None:
            print("Could not access RTP payloader src pad for timestamp embedding.")
            return False
        pad.add_probe(Gst.PadProbeType.BUFFER, self._on_rtp_out)
        return True

    def _add_extension(self, rtp, payload: bytes) -> bool:
        attempts = (
            lambda: rtp.add_extension_onebyte_header(RTP_EXTENSION_ID, payload, len(payload)),
            lambda: rtp.add_extension_onebyte_header(RTP_EXTENSION_ID, list(payload), len(payload)),
            lambda: rtp.add_extension_onebyte_header(RTP_EXTENSION_ID, payload),
            lambda: rtp.add_extension_onebyte_header(RTP_EXTENSION_ID, list(payload)),
        )
        for attempt in attempts:
            try:
                return bool(attempt())
            except TypeError:
                continue
        return False

    def _on_rtp_out(self, pad, info):
        buffer = info.get_buffer()
        if buffer is None:
            return Gst.PadProbeReturn.OK

        success, rtp = GstRtp.RTPBuffer.map(buffer, Gst.MapFlags.READWRITE)
        if not success:
            self._warn_extension_once("Could not map RTP buffer READWRITE; timestamp extension was not embedded.")
            return Gst.PadProbeReturn.OK

        try:
            seq = rtp.get_seq()
            timestamp_ns = time.time_ns()
            payload = ECHO_STRUCT.pack(seq, timestamp_ns)
            self.sent_packets += 1
            if self._add_extension(rtp, payload):
                self.extension_packets += 1
            else:
                self._warn_extension_once("Could not add RTP one-byte header extension.")
        finally:
            rtp.unmap()

        return Gst.PadProbeReturn.OK

    def _warn_extension_once(self, message: str):
        now_ns = time.time_ns()
        if now_ns - self.last_extension_warning_ns >= 2_000_000_000:
            print("Warning: " + message)
            self.last_extension_warning_ns = now_ns

    def _start_echo_server(self):
        self.echo_running = True
        self.echo_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.echo_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.echo_server.settimeout(1.0)
        self.echo_server.bind(("0.0.0.0", self.args.echo_port))
        self.echo_server.listen(1)
        print(f"RTT echo TCP server listening on 0.0.0.0:{self.args.echo_port}")

        self.echo_thread = threading.Thread(target=self._echo_server_loop, daemon=True)
        self.echo_thread.start()

    def _echo_server_loop(self):
        conn = None
        pending = bytearray()

        while self.echo_running:
            try:
                if conn is None:
                    conn, addr = self.echo_server.accept()
                    conn.settimeout(0.2)
                    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    pending.clear()
                    print(f"RTT echo client connected from {addr[0]}:{addr[1]}")

                chunk = conn.recv(4096)
                if not chunk:
                    conn.close()
                    conn = None
                    continue

                pending.extend(chunk)
                while len(pending) >= ECHO_STRUCT.size:
                    packet = bytes(pending[: ECHO_STRUCT.size])
                    del pending[: ECHO_STRUCT.size]
                    self._handle_echo_packet(packet)

            except socket.timeout:
                continue
            except OSError as exc:
                if self.echo_running:
                    print(f"RTT echo server socket error: {exc}")
                if conn is not None:
                    try:
                        conn.close()
                    except OSError:
                        pass
                    conn = None
            except Exception as exc:
                if self.echo_running:
                    print(f"RTT echo server error: {exc}")
                if conn is not None:
                    try:
                        conn.close()
                    except OSError:
                        pass
                    conn = None

        if conn is not None:
            conn.close()

    def _handle_echo_packet(self, packet: bytes):
        seq, timestamp_ns = ECHO_STRUCT.unpack(packet)
        now_ns = time.time_ns()
        rtt_ms = (now_ns - timestamp_ns) / 1_000_000
        if rtt_ms < 0 or rtt_ms > self.args.max_rtt_ms:
            return

        self.echoed_packets += 1
        self.rtt_window.append(rtt_ms)
        avg_ms = sum(self.rtt_window) / len(self.rtt_window)
        print(
            f"RTT seq={seq:5d} {rtt_ms:8.3f} ms "
            f"(avg{len(self.rtt_window):03d}={avg_ms:8.3f} ms, echoes={self.echoed_packets})"
        )

    def _stop_echo_server(self):
        self.echo_running = False
        if self.echo_server is not None:
            try:
                self.echo_server.close()
            except OSError:
                pass
            self.echo_server = None

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

        self._start_echo_server()

        print(f"Streaming camera to {self.args.client_ip}:{self.args.udp_port}")
        print(f"Video caps: {self._source_caps()}")
        print(f"Encoder bitrate: {self.args.bitrate} kbps")
        print("Press Ctrl+C to stop.")

        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            print("Unable to set sender pipeline to PLAYING.")
            self.stop()
            return False

        self.loop.run()
        return True

    def stop(self):
        with self.stop_lock:
            if self.stopping:
                return
            self.stopping = True

        self._stop_echo_server()

        if self.pipeline is not None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None

        print(
            "Final counts: "
            f"rtp_sent={self.sent_packets}, "
            f"timestamp_extensions={self.extension_packets}, "
            f"echoes={self.echoed_packets}"
        )

        if self.loop is not None and self.loop.is_running():
            self.loop.quit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Windows GStreamer server: stream camera video and print timestamp echo RTT."
    )
    parser.add_argument("--client-ip", default=DEFAULT_CLIENT_IP)
    parser.add_argument("--udp-port", type=int, default=DEFAULT_UDP_PORT)
    parser.add_argument("--echo-port", type=int, default=DEFAULT_ECHO_PORT)
    parser.add_argument("--source-element", default="ksvideosrc")
    parser.add_argument("--device-index", type=int, default=0)
    parser.add_argument("--device-name", default=None)
    parser.add_argument("--device-path", default=None)
    parser.add_argument("--input-format", default="image/jpeg", choices=["image/jpeg", "video/x-raw"])
    parser.add_argument("--source-caps", default=None, help="Full source caps override.")
    parser.add_argument("--width", type=int, default=1920)
    parser.add_argument("--height", type=int, default=1080)
    parser.add_argument("--fps", type=int, default=30)
    parser.add_argument("--bitrate", type=int, default=30000)
    parser.add_argument("--key-int-max", type=int, default=60)
    parser.add_argument("--payload-type", type=int, default=96)
    parser.add_argument("--avg-window", type=int, default=100)
    parser.add_argument("--max-rtt-ms", type=float, default=5000.0)
    return parser.parse_args()


def main() -> int:
    server = WindowsTimestampEchoServer(parse_args())
    return 0 if server.start() else 1


if __name__ == "__main__":
    sys.exit(main())
