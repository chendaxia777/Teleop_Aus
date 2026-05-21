#!/usr/bin/env python3
"""
GStreamer Video Sender Script

Captures video from a V4L2 device, encodes it with H.264, and streams via UDP/RTP.

Equivalent to:
gst-launch-1.0 -e -v v4l2src device=/dev/video0 do-timestamp=true ! \
    image/jpeg,width=1920,height=1080,framerate=30/1 ! jpegdec ! videoconvert ! \
    queue max-size-buffers=1 leaky=downstream ! \
    x264enc tune=zerolatency speed-preset=ultrafast bitrate=6000 key-int-max=60 bframes=0 ! \
    h264parse config-interval=1 ! rtph264pay pt=96 config-interval=1 ! \
    udpsink host=10.246.169.65 port=5004 sync=false async=false
"""

import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstRtp', '1.0')
from gi.repository import Gst, GLib, GstRtp
import sys
import signal
import json
import os
import time
import threading
import socket
import struct


def load_config(config_path=None):
    """
    Load configuration from JSON file.
    
    Args:
        config_path: Path to config file. If None, uses default location.
    
    Returns:
        dict: Configuration dictionary
    """
    if config_path is None:
        # Default config path: config/gst_config.json relative to this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "config", "gst_simulation.json")
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return json.load(f)


class GStreamerSender:
    def __init__(self, device="/dev/video0", host="10.20.0.254", port=5004,
                 width=1920, height=1080, framerate=30, bitrate=6000):
        """
        Initialize the GStreamer sender pipeline.
        
        Args:
            device: V4L2 device path (e.g., /dev/video0)
            host: Destination IP address
            port: Destination UDP port
            width: Video width in pixels
            height: Video height in pixels
            framerate: Video framerate
            bitrate: H.264 encoding bitrate in kbps
        """
        self.device = device
        self.host = host
        self.port = port
        self.width = width
        self.height = height
        self.framerate = framerate
        self.bitrate = bitrate
        
        self.pipeline = None
        self.loop = None
        
        # RTT measurement
        self.rtt_port = port + 1  # TCP port for RTT echo (UDP port + 1)
        self.t1 = {}  # Dict: seq -> send_time_ns
        self.t1_lock = threading.Lock()
        self.tcp_server = None
        self.tcp_running = False
        
        # RTT statistics
        self._rtt_sum_ms = 0.0
        self._rtt_count = 0
        self._last_rtt_log_ns = 0
        
        # Store all RTT values for final statistics
        self._all_rtt_values = []
        self._all_rtt_lock = threading.Lock()
        
        # Packet loss tracking
        self._packets_sent = 0
        self._packets_received = 0
        self._packet_loss_lock = threading.Lock()
        
    def create_pipeline(self):
        """Create the GStreamer pipeline."""
        # Initialize GStreamer
        Gst.init(None)
        
        # Create pipeline
        self.pipeline = Gst.Pipeline.new("video-sender")
        
        # Create elements
        # Source: V4L2 video capture
        source = Gst.ElementFactory.make("v4l2src", "source")
        source.set_property("device", self.device)
        source.set_property("do-timestamp", True)
        
        # Caps filter for MJPEG input
        caps_filter = Gst.ElementFactory.make("capsfilter", "caps_filter")
        caps = Gst.Caps.from_string(
            f"image/jpeg,width={self.width},height={self.height},framerate={self.framerate}/1"
        )
        caps_filter.set_property("caps", caps)
        
        # JPEG decoder
        jpegdec = Gst.ElementFactory.make("jpegdec", "jpegdec")
        
        # Video converter
        videoconvert = Gst.ElementFactory.make("videoconvert", "videoconvert")
        
        # Queue with leaky downstream buffer
        queue = Gst.ElementFactory.make("queue", "queue")
        queue.set_property("max-size-buffers", 1)
        queue.set_property("leaky", 2)  # 2 = downstream
        
        # H.264 encoder (x264enc)
        encoder = Gst.ElementFactory.make("x264enc", "encoder")
        encoder.set_property("tune", 0x00000004)  # zerolatency
        encoder.set_property("speed-preset", 1)   # ultrafast
        encoder.set_property("bitrate", self.bitrate)
        encoder.set_property("key-int-max", 60)
        encoder.set_property("bframes", 0)
        
        # H.264 parser
        h264parse = Gst.ElementFactory.make("h264parse", "h264parse")
        h264parse.set_property("config-interval", 1)
        
        # RTP H.264 payloader
        rtppay = Gst.ElementFactory.make("rtph264pay", "rtppay")
        rtppay.set_property("pt", 96)
        rtppay.set_property("config-interval", 1)
        
        # UDP sink
        udpsink = Gst.ElementFactory.make("udpsink", "udpsink")
        udpsink.set_property("host", self.host)
        udpsink.set_property("port", self.port)
        udpsink.set_property("sync", False)
        udpsink.set_property("async", False)
        
        # Check all elements were created successfully
        elements = [source, caps_filter, jpegdec, videoconvert, queue, 
                    encoder, h264parse, rtppay, udpsink]
        element_names = ["v4l2src", "capsfilter", "jpegdec", "videoconvert", 
                         "queue", "x264enc", "h264parse", "rtph264pay", "udpsink"]
        
        for elem, name in zip(elements, element_names):
            if elem is None:
                print(f"Error: Could not create element '{name}'")
                return False
        
        # Add elements to pipeline
        for elem in elements:
            self.pipeline.add(elem)
        
        # Link elements
        if not source.link(caps_filter):
            print("Error: Could not link source to caps_filter")
            return False
        if not caps_filter.link(jpegdec):
            print("Error: Could not link caps_filter to jpegdec")
            return False
        if not jpegdec.link(videoconvert):
            print("Error: Could not link jpegdec to videoconvert")
            return False
        if not videoconvert.link(queue):
            print("Error: Could not link videoconvert to queue")
            return False
        if not queue.link(encoder):
            print("Error: Could not link queue to encoder")
            return False
        if not encoder.link(h264parse):
            print("Error: Could not link encoder to h264parse")
            return False
        if not h264parse.link(rtppay):
            print("Error: Could not link h264parse to rtppay")
            return False
        if not rtppay.link(udpsink):
            print("Error: Could not link rtppay to udpsink")
            return False
        
        # Add pad probe to record send timestamps
        rtppay_src_pad = rtppay.get_static_pad("src")
        if rtppay_src_pad:
            rtppay_src_pad.add_probe(Gst.PadProbeType.BUFFER, self._on_rtp_out)
        
        return True
    
    def _on_rtp_out(self, pad, info):
        """Record timestamp when RTP packet is sent."""
        buffer = info.get_buffer()
        if buffer is None:
            return Gst.PadProbeReturn.OK
        
        success, rtp = GstRtp.RTPBuffer.map(buffer, Gst.MapFlags.READ)
        if not success:
            return Gst.PadProbeReturn.OK
        
        try:
            seq = rtp.get_seq()
            now_ns = time.time_ns()
            with self.t1_lock:
                self.t1[seq] = now_ns
                # Clean up old entries based on time (older than 5 seconds)
                # This handles seq number wraparound (16-bit, 0-65535)
                cutoff_ns = now_ns - 5_000_000_000  # 5 seconds
                keys_to_delete = [k for k, v in self.t1.items() if v < cutoff_ns]
                for k in keys_to_delete:
                    del self.t1[k]
            
            # Increment packets sent counter
            with self._packet_loss_lock:
                self._packets_sent += 1
        finally:
            rtp.unmap()
        
        return Gst.PadProbeReturn.OK
    
    def _start_tcp_server(self):
        """Start TCP server to receive seq echoes from receiver."""
        self.tcp_running = True
        self.tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_server.settimeout(1.0)
        self.tcp_server.bind(('0.0.0.0', self.rtt_port))
        self.tcp_server.listen(1)
        print(f"RTT TCP server listening on port {self.rtt_port}")
        
        def server_thread():
            conn = None
            while self.tcp_running:
                try:
                    if conn is None:
                        conn, addr = self.tcp_server.accept()
                        conn.settimeout(0.1)
                        print(f"RTT client connected from {addr}")
                    
                    # Receive seq numbers (2 bytes each, unsigned short)
                    data = conn.recv(2)
                    if not data:
                        conn.close()
                        conn = None
                        continue
                    
                    if len(data) == 2:
                        seq = struct.unpack('!H', data)[0]
                        now_ns = time.time_ns()
                        
                        with self.t1_lock:
                            if seq in self.t1:
                                rtt_ns = now_ns - self.t1[seq]
                                rtt_ms = rtt_ns / 1_000_000
                                del self.t1[seq]
                                
                                # Sanity check: RTT should be positive and less than 5 seconds
                                # This filters out invalid values from seq number wraparound
                                if rtt_ms < 0 or rtt_ms > 5000:
                                    continue
                                
                                # Only count as received if we found matching sent record
                                with self._packet_loss_lock:
                                    self._packets_received += 1
                                
                                self._rtt_sum_ms += rtt_ms
                                self._rtt_count += 1
                                
                                # Store RTT value for final statistics
                                with self._all_rtt_lock:
                                    self._all_rtt_values.append(rtt_ms)
                                
                                # Log every second
                                if now_ns - self._last_rtt_log_ns >= 1_000_000_000:
                                    avg_ms = self._rtt_sum_ms / max(self._rtt_count, 1)
                                    print(f"RTT: {rtt_ms:.2f} ms (avg {avg_ms:.2f} ms, n={self._rtt_count})")
                                    self._last_rtt_log_ns = now_ns
                                    self._rtt_sum_ms = 0.0
                                    self._rtt_count = 0
                
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.tcp_running:
                        print(f"TCP server error: {e}")
                    if conn:
                        conn.close()
                        conn = None
            
            if conn:
                conn.close()
        
        thread = threading.Thread(target=server_thread, daemon=True)
        thread.start()
    
    def _stop_tcp_server(self):
        """Stop the TCP server."""
        self.tcp_running = False
        if self.tcp_server:
            self.tcp_server.close()
            self.tcp_server = None
    
    def on_message(self, bus, message):
        """Handle pipeline messages."""
        msg_type = message.type
        
        if msg_type == Gst.MessageType.EOS:
            print("End of stream reached")
            self.stop()
        elif msg_type == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            print(f"Error: {err.message}")
            if debug:
                print(f"Debug info: {debug}")
            self.stop()
        elif msg_type == Gst.MessageType.WARNING:
            warn, debug = message.parse_warning()
            print(f"Warning: {warn.message}")
            if debug:
                print(f"Debug info: {debug}")
        elif msg_type == Gst.MessageType.STATE_CHANGED:
            if message.src == self.pipeline:
                old_state, new_state, pending = message.parse_state_changed()
                print(f"Pipeline state changed: {old_state.value_nick} -> {new_state.value_nick}")
        
        return True
    
    def start(self):
        """Start the pipeline."""
        if not self.create_pipeline():
            print("Failed to create pipeline")
            return False
        
        # Set up message bus
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self.on_message)
        
        # Create main loop
        self.loop = GLib.MainLoop()
        
        # Set up signal handlers for graceful shutdown
        def signal_handler(sig, frame):
            print("\nInterrupt received, stopping...")
            self.stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start TCP server for RTT measurement
        self._start_tcp_server()
        
        # Start playing
        print(f"Starting video stream from {self.device} to {self.host}:{self.port}")
        print(f"Resolution: {self.width}x{self.height} @ {self.framerate}fps")
        print(f"Bitrate: {self.bitrate} kbps")
        print("Press Ctrl+C to stop...")
        
        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            print("Unable to set pipeline to playing state")
            return False
        
        try:
            self.loop.run()
        except Exception as e:
            print(f"Error running main loop: {e}")
            self.stop()
        
        return True
    
    def _print_final_rtt_statistics(self):
        """Print final RTT statistics (mean and std)."""
        with self._all_rtt_lock:
            if len(self._all_rtt_values) == 0:
                print("\n=== Final RTT Statistics ===")
                print("No RTT measurements recorded.")
                return
            
            n = len(self._all_rtt_values)
            mean_rtt = sum(self._all_rtt_values) / n
            
            # Calculate sample standard deviation (n-1 for unbiased estimator)
            if n > 1:
                variance = sum((x - mean_rtt) ** 2 for x in self._all_rtt_values) / (n - 1)
                std_rtt = variance ** 0.5
            else:
                std_rtt = 0.0
            
            print("\n=== Final RTT Statistics ===")
            print(f"Total samples: {n}")
            print(f"Mean RTT: {mean_rtt:.2f} ms")
            print(f"Std RTT:  {std_rtt:.2f} ms")
            print("============================\n")
        
        # Print packet loss statistics
        with self._packet_loss_lock:
            print("=== Packet Loss Statistics ===")
            print(f"Packets sent:     {self._packets_sent}")
            print(f"Packets received: {self._packets_received}")
            if self._packets_sent > 0:
                loss_rate = (self._packets_sent - self._packets_received) / self._packets_sent * 100
                print(f"Packet loss rate: {loss_rate:.2f}%")
            else:
                print("Packet loss rate: N/A (no packets sent)")
            print("==============================")
    
    def stop(self):
        """Stop the pipeline gracefully."""
        # Print final RTT statistics
        self._print_final_rtt_statistics()
        
        # Stop TCP server
        self._stop_tcp_server()
        
        if self.pipeline:
            print("Stopping pipeline...")
            # Send EOS to ensure proper cleanup
            self.pipeline.send_event(Gst.Event.new_eos())
            # Give it a moment to process EOS
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None
        
        if self.loop and self.loop.is_running():
            self.loop.quit()


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="GStreamer Video Sender - Stream video via UDP/RTP"
    )
    parser.add_argument(
        "--config", "-c",
        default=None,
        help="Path to JSON config file (default: config/gst_config.json)"
    )
    
    args = parser.parse_args()
    
    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing config file: {e}")
        sys.exit(1)
    
    # Extract sender-specific and common parameters
    sender_config = config.get("sender", {})
    common_config = config.get("common", {})
    
    sender = GStreamerSender(
        device=sender_config.get("device", "/dev/video0"),
        host=sender_config.get("destination_ip", "10.246.169.65"),
        port=common_config.get("udp_port", 5004),
        width=sender_config.get("width", 1920),
        height=sender_config.get("height", 1080),
        framerate=sender_config.get("framerate", 30),
        bitrate=sender_config.get("bitrate", 6000)
    )
    
    sender.start()


if __name__ == "__main__":
    main()
