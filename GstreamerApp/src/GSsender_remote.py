#!/usr/bin/env python3
"""
GStreamer Video Sender Script

Captures video from a configurable camera source, encodes it with H.264, and
streams via UDP/RTP.

Equivalent to:
gst-launch-1.0 -e -v ksvideosrc device-index=0 do-timestamp=true ! \
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
import csv
import datetime


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
        config_path = os.path.join(script_dir, "config", "gst_remote.json")
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return json.load(f)


class GStreamerSender:
    def __init__(self, device="/dev/video0", host="10.20.0.254", port=5004,
                 width=1920, height=1080, framerate=30, bitrate=6000,
                 source_element=None, device_index=0, device_name=None,
                 device_path=None, input_format="image/jpeg", source_caps=None,
                 output_dir=None, filename_prefix="gstreamer_metrics",
                 enable_video_recording=False, video_filename_prefix="video_recording"):
        """
        Initialize the GStreamer sender pipeline.
        
        Args:
            device: V4L2/DirectShow device path when the selected source supports it
            host: Destination IP address
            port: Destination UDP port
            width: Video width in pixels
            height: Video height in pixels
            framerate: Video framerate
            bitrate: H.264 encoding bitrate in kbps
            source_element: GStreamer camera source element (ksvideosrc on Windows)
            device_index: Zero-based camera index for Windows camera sources
            device_name: Human-readable camera name for Windows camera sources
            device_path: Device path for Windows camera sources
            input_format: Source media type, e.g. image/jpeg or video/x-raw
            source_caps: Complete caps string override for the camera source
            output_dir: Directory for saving metrics CSV and video files
            filename_prefix: Prefix for metrics CSV filename
            enable_video_recording: Enable video file recording
            video_filename_prefix: Prefix for video recording filename
        """
        self.device = device
        self.host = host
        self.port = port
        self.width = width
        self.height = height
        self.framerate = framerate
        self.bitrate = bitrate
        self.source_element = source_element or ("ksvideosrc" if os.name == "nt" else "v4l2src")
        self.device_index = device_index
        self.device_name = device_name
        self.device_path = device_path
        self.input_format = input_format or "image/jpeg"
        self.source_caps = source_caps
        
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
        
        # Logging configuration
        self.output_dir = output_dir or os.path.join(
            os.path.expanduser('~'), 'work/Telemanipulation_Ros/results')
        self.filename_prefix = filename_prefix
        self.enable_video_recording = enable_video_recording
        self.video_filename_prefix = video_filename_prefix
        
        # Session timestamp for filenames
        self.session_start = datetime.datetime.now()
        self.session_timestamp = self.session_start.strftime("%Y-%m-%d-%H-%M-%S")
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # CSV logging
        self.csv_path = os.path.join(
            self.output_dir, f"{self.filename_prefix}_{self.session_timestamp}.csv")
        self.csv_file = None
        self.csv_writer = None
        self.csv_lock = threading.Lock()
        
        # Video recording path
        self.video_path = os.path.join(
            self.output_dir, f"{self.video_filename_prefix}_{self.session_timestamp}.mp4")
        
        # Packet loss rate logging interval (seconds)
        self._last_plr_log_ns = 0
        self._plr_log_interval_ns = 60_000_000_000  # 60 seconds
        
        # Stopping guard
        self._stopping = False

    def _set_optional_property(self, element, property_name, value):
        """Set a GStreamer property only when the element exposes it."""
        if value is None:
            return
        if element.find_property(property_name):
            element.set_property(property_name, value)

    def _create_source(self):
        """Create the configured camera source, with Windows-friendly fallbacks."""
        candidates = [self.source_element]
        if os.name == "nt":
            candidates.extend(["ksvideosrc", "dshowvideosrc", "mfvideosrc", "autovideosrc"])
        else:
            candidates.extend(["v4l2src", "autovideosrc"])

        seen = set()
        for element_name in candidates:
            if not element_name or element_name in seen:
                continue
            seen.add(element_name)

            source = Gst.ElementFactory.make(element_name, "source")
            if source is None:
                continue

            self._set_optional_property(source, "do-timestamp", True)
            self._set_optional_property(source, "device-index", self.device_index)
            self._set_optional_property(source, "device-name", self.device_name)
            self._set_optional_property(source, "device-path", self.device_path)

            if element_name == "v4l2src":
                self._set_optional_property(source, "device", self.device)
            elif element_name == "dshowvideosrc":
                self._set_optional_property(source, "device", self.device_path)

            print(f"Using video source element: {element_name}")
            return source, element_name

        print(f"Error: Could not create any video source from candidates: {', '.join(candidates)}")
        return None, None

    def _build_source_caps(self):
        """Build source caps from config, allowing a full caps override."""
        if self.source_caps:
            return self.source_caps

        caps_parts = [
            self.input_format,
            f"width={self.width}",
            f"height={self.height}",
            f"framerate={self.framerate}/1",
        ]
        return ",".join(caps_parts)
    
    def _init_csv(self):
        """Initialize CSV file for logging metrics."""
        self.csv_file = open(self.csv_path, 'w', newline='')
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow([
            'timestamp', 'elapsed_seconds', 'metric_type', 'value'
        ])
        self.csv_file.flush()
        print(f"Metrics CSV file: {self.csv_path}")
    
    def _write_csv_row(self, metric_type: str, value: float):
        """Write a row to the CSV file."""
        now = datetime.datetime.now()
        elapsed = (now - self.session_start).total_seconds()
        
        with self.csv_lock:
            if self.csv_writer:
                self.csv_writer.writerow([
                    now.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    f'{elapsed:.3f}',
                    metric_type,
                    f'{value:.4f}'
                ])
                self.csv_file.flush()
    
    def _close_csv(self):
        """Close the CSV file."""
        with self.csv_lock:
            if self.csv_file:
                self.csv_file.close()
                self.csv_file = None
                self.csv_writer = None
        
    def create_pipeline(self):
        """Create the GStreamer pipeline."""
        # Initialize GStreamer
        Gst.init(None)
        
        # Create pipeline
        self.pipeline = Gst.Pipeline.new("video-sender")
        
        # Create elements
        # Source: configurable camera capture. On Windows, gst_remote.json uses ksvideosrc.
        source, source_name = self._create_source()
        
        # Caps filter for camera input
        caps_filter = Gst.ElementFactory.make("capsfilter", "caps_filter")
        caps = Gst.Caps.from_string(self._build_source_caps())
        caps_filter.set_property("caps", caps)
        
        # JPEG decoder is only needed when the camera outputs MJPEG.
        needs_jpegdec = self.input_format == "image/jpeg"
        jpegdec = Gst.ElementFactory.make("jpegdec", "jpegdec") if needs_jpegdec else None
        
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
        
        # Tee for splitting encoded stream (for video recording)
        tee = None
        queue_stream = None
        queue_file = None
        mp4mux = None
        filesink = None
        
        if self.enable_video_recording:
            # Create tee element to split the stream
            tee = Gst.ElementFactory.make("tee", "tee")
            
            # Queue for streaming path
            queue_stream = Gst.ElementFactory.make("queue", "queue_stream")
            queue_stream.set_property("max-size-buffers", 1)
            queue_stream.set_property("leaky", 2)  # downstream
            
            # Queue for file recording path
            queue_file = Gst.ElementFactory.make("queue", "queue_file")
            queue_file.set_property("max-size-buffers", 100)
            
            # H264 parser for file recording (need separate instance)
            h264parse_file = Gst.ElementFactory.make("h264parse", "h264parse_file")
            h264parse_file.set_property("config-interval", 1)
            
            # MP4 muxer
            mp4mux = Gst.ElementFactory.make("mp4mux", "mp4mux")
            mp4mux.set_property("faststart", True)
            
            # File sink
            filesink = Gst.ElementFactory.make("filesink", "filesink")
            filesink.set_property("location", self.video_path)
            filesink.set_property("sync", False)
            filesink.set_property("async", False)
            
            print(f"Video recording enabled: {self.video_path}")
        
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
        elements = [source, caps_filter]
        element_names = [source_name or self.source_element, "capsfilter"]
        if needs_jpegdec:
            elements.append(jpegdec)
            element_names.append("jpegdec")
        elements.extend([videoconvert, queue, encoder, h264parse, rtppay, udpsink])
        element_names.extend(["videoconvert", "queue", "x264enc", "h264parse", "rtph264pay", "udpsink"])
        
        for elem, name in zip(elements, element_names):
            if elem is None:
                print(f"Error: Could not create element '{name}'")
                return False
        
        # Check video recording elements if enabled
        if self.enable_video_recording:
            recording_elements = [tee, queue_stream, queue_file, h264parse_file, mp4mux, filesink]
            recording_names = ["tee", "queue_stream", "queue_file", "h264parse_file", "mp4mux", "filesink"]
            for elem, name in zip(recording_elements, recording_names):
                if elem is None:
                    print(f"Error: Could not create recording element '{name}'")
                    return False
        
        # Add elements to pipeline
        for elem in elements:
            self.pipeline.add(elem)
        
        # Add video recording elements if enabled
        if self.enable_video_recording:
            for elem in [tee, queue_stream, queue_file, h264parse_file, mp4mux, filesink]:
                self.pipeline.add(elem)
        
        # Link elements - basic chain up to encoder
        if not source.link(caps_filter):
            print("Error: Could not link source to caps_filter")
            return False
        if needs_jpegdec:
            if not caps_filter.link(jpegdec):
                print("Error: Could not link caps_filter to jpegdec")
                return False
            if not jpegdec.link(videoconvert):
                print("Error: Could not link jpegdec to videoconvert")
                return False
        elif not caps_filter.link(videoconvert):
            print("Error: Could not link caps_filter to videoconvert")
            return False
        if not videoconvert.link(queue):
            print("Error: Could not link videoconvert to queue")
            return False
        if not queue.link(encoder):
            print("Error: Could not link queue to encoder")
            return False
        
        # Link encoder output based on video recording mode
        if self.enable_video_recording:
            # encoder -> tee
            if not encoder.link(tee):
                print("Error: Could not link encoder to tee")
                return False
            
            # Streaming branch: tee -> queue_stream -> h264parse -> rtppay -> udpsink
            tee_pad_stream = tee.request_pad_simple("src_%u")
            queue_stream_sink_pad = queue_stream.get_static_pad("sink")
            if tee_pad_stream.link(queue_stream_sink_pad) != Gst.PadLinkReturn.OK:
                print("Error: Could not link tee to queue_stream")
                return False
            
            if not queue_stream.link(h264parse):
                print("Error: Could not link queue_stream to h264parse")
                return False
            if not h264parse.link(rtppay):
                print("Error: Could not link h264parse to rtppay")
                return False
            if not rtppay.link(udpsink):
                print("Error: Could not link rtppay to udpsink")
                return False
            
            # Recording branch: tee -> queue_file -> h264parse_file -> mp4mux -> filesink
            tee_pad_file = tee.request_pad_simple("src_%u")
            queue_file_sink_pad = queue_file.get_static_pad("sink")
            if tee_pad_file.link(queue_file_sink_pad) != Gst.PadLinkReturn.OK:
                print("Error: Could not link tee to queue_file")
                return False
            
            if not queue_file.link(h264parse_file):
                print("Error: Could not link queue_file to h264parse_file")
                return False
            if not h264parse_file.link(mp4mux):
                print("Error: Could not link h264parse_file to mp4mux")
                return False
            if not mp4mux.link(filesink):
                print("Error: Could not link mp4mux to filesink")
                return False
        else:
            # No video recording: encoder -> h264parse -> rtppay -> udpsink
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
                                
                                # Write RTT to CSV
                                self._write_csv_row('rtt_ms', rtt_ms)
                                
                                # Log packet loss rate periodically (every 60 seconds)
                                if now_ns - self._last_plr_log_ns >= self._plr_log_interval_ns:
                                    with self._packet_loss_lock:
                                        if self._packets_sent > 0:
                                            loss_rate = (self._packets_sent - self._packets_received) / self._packets_sent * 100
                                            self._write_csv_row('packet_loss_rate', loss_rate)
                                            print(f"Packet loss rate: {loss_rate:.2f}% (sent: {self._packets_sent}, received: {self._packets_received})")
                                    self._last_plr_log_ns = now_ns
                                
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
            # EOS is handled by stop() - don't call stop() here to avoid recursion
            print("End of stream reached (handled)")
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
        # Initialize CSV logging
        self._init_csv()
        
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
        print(f"Starting video stream from {self.source_element} to {self.host}:{self.port}")
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
        # Guard against multiple calls
        if self._stopping:
            return
        self._stopping = True
        
        # Write final packet loss rate to CSV
        with self._packet_loss_lock:
            if self._packets_sent > 0:
                final_loss_rate = (self._packets_sent - self._packets_received) / self._packets_sent * 100
                self._write_csv_row('final_packet_loss_rate', final_loss_rate)
        
        # Print final RTT statistics
        self._print_final_rtt_statistics()
        
        # Close CSV file
        self._close_csv()
        print(f"Metrics saved to: {self.csv_path}")
        if self.enable_video_recording:
            print(f"Video saved to: {self.video_path}")
        
        # Stop TCP server
        self._stop_tcp_server()
        
        if self.pipeline:
            print("Stopping pipeline...")
            # Send EOS to ensure proper cleanup (required for mp4mux to write moov atom)
            self.pipeline.send_event(Gst.Event.new_eos())
            
            # Wait for EOS to propagate through the pipeline
            # This is critical for MP4 muxer to finalize the file
            if self.enable_video_recording:
                print("Waiting for video file to finalize...")
                bus = self.pipeline.get_bus()
                # Wait up to 5 seconds for EOS message
                msg = bus.timed_pop_filtered(5 * Gst.SECOND, Gst.MessageType.EOS | Gst.MessageType.ERROR)
                if msg:
                    if msg.type == Gst.MessageType.EOS:
                        print("EOS received, video file finalized.")
                    elif msg.type == Gst.MessageType.ERROR:
                        err, debug = msg.parse_error()
                        print(f"Error during finalization: {err.message}")
                else:
                    print("Warning: Timeout waiting for EOS, video file may be incomplete.")
            
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
    # Get sender-specific logging config (nested under sender)
    sender_logging_config = sender_config.get("logging", {})

    sender = GStreamerSender(
        device=sender_config.get("device", "/dev/video0"),
        host=sender_config.get("destination_ip", "10.246.169.65"),
        port=common_config.get("udp_port", 5004),
        width=sender_config.get("width", 1920),
        height=sender_config.get("height", 1080),
        framerate=sender_config.get("framerate", 30),
        bitrate=sender_config.get("bitrate", 6000),
        source_element=sender_config.get("source_element"),
        device_index=sender_config.get("device_index", 0),
        device_name=sender_config.get("device_name"),
        device_path=sender_config.get("device_path"),
        input_format=sender_config.get("input_format", "image/jpeg"),
        source_caps=sender_config.get("source_caps"),
        output_dir=sender_logging_config.get("output_dir"),
        filename_prefix=sender_logging_config.get("filename_prefix", "gstreamer_metrics"),
        enable_video_recording=sender_logging_config.get("enable_video_recording", False),
        video_filename_prefix=sender_logging_config.get("video_filename_prefix", "sender_video")
    )
    
    sender.start()


if __name__ == "__main__":
    main()
