#!/usr/bin/env python3
"""
GStreamer Video Receiver Script

Receives H.264 encoded video via UDP/RTP and displays it.

Equivalent to:
GST_TRACERS="latency" GST_DEBUG="GST_TRACER:7" gst-launch-1.0 -e \
    udpsrc port=5004 caps="application/x-rtp,media=video,encoding-name=H264,payload=96" ! \
    rtpjitterbuffer latency=0 ! rtph264depay ! h264parse ! avdec_h264 ! \
    videoconvert ! autovideosink sync=false
"""

import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstRtp', '1.0')
from gi.repository import Gst, GLib, GstRtp
import sys
import signal
import os
import json
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
        config_path = os.path.join(script_dir, "config", "gst_local.json")
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return json.load(f)


class GStreamerReceiver:
    def __init__(self, port=5004, jitterbuffer_latency=0, enable_latency_tracer=True,
                 enable_recording=False, output_dir=None, video_filename_prefix="video_recording"):
        """
        Initialize the GStreamer receiver pipeline.

        Args:
            port: UDP port to listen on
            jitterbuffer_latency: Jitter buffer latency in ms (0 for minimum latency)
            enable_latency_tracer: Enable GStreamer latency tracer for debugging
            enable_recording: Enable video recording to file
            output_dir: Directory to save recorded videos
            video_filename_prefix: Prefix for recorded video filenames
        """
        self.port = port
        self.jitterbuffer_latency = jitterbuffer_latency
        self.enable_latency_tracer = enable_latency_tracer
        self.enable_recording = enable_recording
        self.output_dir = output_dir
        self.video_filename_prefix = video_filename_prefix

        self.pipeline = None
        self.loop = None

        # RTT echo client
        self.sender_host = None  # Will be set from config
        self.rtt_port = port + 1  # TCP port for RTT echo (UDP port + 1)
        self.tcp_client = None
        self.tcp_connected = False
        self.tcp_lock = threading.Lock()
        
    def create_pipeline(self):
        """Create the GStreamer pipeline."""
        # Set up latency tracer environment variables before Gst.init()
        if self.enable_latency_tracer:
            os.environ["GST_TRACERS"] = "latency"
            os.environ["GST_DEBUG"] = "GST_TRACER:7"

        # Initialize GStreamer
        Gst.init(None)

        # Create pipeline
        self.pipeline = Gst.Pipeline.new("video-receiver")

        # Create elements
        # Source: UDP receiver
        udpsrc = Gst.ElementFactory.make("udpsrc", "udpsrc")
        udpsrc.set_property("port", self.port)

        # Set caps for RTP H264
        caps = Gst.Caps.from_string(
            "application/x-rtp,media=video,encoding-name=H264,payload=96"
        )
        udpsrc.set_property("caps", caps)

        # RTP jitter buffer
        jitterbuffer = Gst.ElementFactory.make("rtpjitterbuffer", "jitterbuffer")
        jitterbuffer.set_property("latency", self.jitterbuffer_latency)

        # RTP H264 depayloader
        rtpdepay = Gst.ElementFactory.make("rtph264depay", "rtpdepay")

        # H.264 parser
        h264parse = Gst.ElementFactory.make("h264parse", "h264parse")

        # Tee element to split stream for display and recording
        tee = Gst.ElementFactory.make("tee", "tee")

        # Display branch
        # Queue for display branch
        queue_display = Gst.ElementFactory.make("queue", "queue_display")

        # H.264 decoder (libav/ffmpeg)
        decoder = Gst.ElementFactory.make("avdec_h264", "decoder")

        # Video converter
        videoconvert = Gst.ElementFactory.make("videoconvert", "videoconvert")

        # Auto video sink (display)
        videosink = Gst.ElementFactory.make("autovideosink", "videosink")
        videosink.set_property("sync", False)

        # Check all common elements were created successfully
        elements = [udpsrc, jitterbuffer, rtpdepay, h264parse, tee,
                    queue_display, decoder, videoconvert, videosink]
        element_names = ["udpsrc", "rtpjitterbuffer", "rtph264depay", "h264parse", "tee",
                         "queue", "avdec_h264", "videoconvert", "autovideosink"]

        for elem, name in zip(elements, element_names):
            if elem is None:
                print(f"Error: Could not create element '{name}'")
                return False

        # Add elements to pipeline
        for elem in elements:
            self.pipeline.add(elem)

        # Link common elements
        if not udpsrc.link(jitterbuffer):
            print("Error: Could not link udpsrc to jitterbuffer")
            return False
        if not jitterbuffer.link(rtpdepay):
            print("Error: Could not link jitterbuffer to rtpdepay")
            return False
        if not rtpdepay.link(h264parse):
            print("Error: Could not link rtpdepay to h264parse")
            return False
        if not h264parse.link(tee):
            print("Error: Could not link h264parse to tee")
            return False

        # Link display branch
        tee_src_pad = tee.get_request_pad("src_%u")
        queue_display_sink_pad = queue_display.get_static_pad("sink")
        if tee_src_pad.link(queue_display_sink_pad) != Gst.PadLinkReturn.OK:
            print("Error: Could not link tee to queue_display")
            return False

        if not queue_display.link(decoder):
            print("Error: Could not link queue_display to decoder")
            return False
        if not decoder.link(videoconvert):
            print("Error: Could not link decoder to videoconvert")
            return False
        if not videoconvert.link(videosink):
            print("Error: Could not link videoconvert to videosink")
            return False

        # Recording branch (if enabled)
        if self.enable_recording:
            # Create output directory if it doesn't exist
            if self.output_dir:
                os.makedirs(self.output_dir, exist_ok=True)

                # Generate filename with timestamp
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                output_file = os.path.join(self.output_dir,
                                          f"{self.video_filename_prefix}_{timestamp}.mp4")

                # Recording branch elements
                queue_record = Gst.ElementFactory.make("queue", "queue_record")
                h264parse_record = Gst.ElementFactory.make("h264parse", "h264parse_record")
                mp4mux = Gst.ElementFactory.make("mp4mux", "mp4mux")
                filesink = Gst.ElementFactory.make("filesink", "filesink")
                filesink.set_property("location", output_file)

                # Check recording elements
                recording_elements = [queue_record, h264parse_record, mp4mux, filesink]
                recording_names = ["queue_record", "h264parse_record", "mp4mux", "filesink"]

                for elem, name in zip(recording_elements, recording_names):
                    if elem is None:
                        print(f"Error: Could not create recording element '{name}'")
                        print("Recording will be disabled")
                        self.enable_recording = False
                        break

                if self.enable_recording:
                    # Add recording elements to pipeline
                    for elem in recording_elements:
                        self.pipeline.add(elem)

                    # Link recording branch
                    tee_src_pad_record = tee.get_request_pad("src_%u")
                    queue_record_sink_pad = queue_record.get_static_pad("sink")
                    if tee_src_pad_record.link(queue_record_sink_pad) != Gst.PadLinkReturn.OK:
                        print("Error: Could not link tee to queue_record")
                        return False

                    if not queue_record.link(h264parse_record):
                        print("Error: Could not link queue_record to h264parse_record")
                        return False
                    if not h264parse_record.link(mp4mux):
                        print("Error: Could not link h264parse_record to mp4mux")
                        return False
                    if not mp4mux.link(filesink):
                        print("Error: Could not link mp4mux to filesink")
                        return False

                    print(f"Recording enabled: {output_file}")
            else:
                print("Warning: Recording enabled but no output_dir specified")
                self.enable_recording = False

        # Add pad probe to extract seq from RTP packets and echo back
        jitterbuffer_src_pad = jitterbuffer.get_static_pad("src")
        if jitterbuffer_src_pad:
            jitterbuffer_src_pad.add_probe(Gst.PadProbeType.BUFFER, self._on_rtp_in)

        return True
    
    def _on_rtp_in(self, pad, info):
        """Extract seq from incoming RTP packet and echo back to sender."""
        buffer = info.get_buffer()
        if buffer is None:
            return Gst.PadProbeReturn.OK
        
        success, rtp = GstRtp.RTPBuffer.map(buffer, Gst.MapFlags.READ)
        if not success:
            return Gst.PadProbeReturn.OK
        
        try:
            seq = rtp.get_seq()
            self._send_seq_echo(seq)
        finally:
            rtp.unmap()
        
        return Gst.PadProbeReturn.OK
    
    def _send_seq_echo(self, seq):
        """Send seq number back to sender via TCP."""
        if not self.sender_host:
            return
        
        with self.tcp_lock:
            # Connect if not connected
            if not self.tcp_connected or self.tcp_client is None:
                try:
                    self.tcp_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.tcp_client.settimeout(1.0)
                    self.tcp_client.connect((self.sender_host, self.rtt_port))
                    # Use TCP_NODELAY to reduce latency for small packets
                    self.tcp_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    self.tcp_client.setblocking(False)  # Non-blocking for sends
                    self.tcp_connected = True
                    print(f"Connected to RTT server at {self.sender_host}:{self.rtt_port}")
                except Exception as e:
                    self.tcp_client = None
                    self.tcp_connected = False
                    return
            
            # Send seq number (2 bytes, unsigned short, network byte order)
            try:
                data = struct.pack('!H', seq)
                # Use sendall to ensure complete send (handles partial sends)
                self.tcp_client.sendall(data)
            except Exception as e:
                # Connection lost, will reconnect on next packet
                self.tcp_connected = False
                if self.tcp_client:
                    self.tcp_client.close()
                    self.tcp_client = None
    
    def _close_tcp_client(self):
        """Close TCP client connection."""
        with self.tcp_lock:
            if self.tcp_client:
                self.tcp_client.close()
                self.tcp_client = None
            self.tcp_connected = False
    
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
        
        # Start playing
        print(f"Starting video receiver on port {self.port}")
        print(f"Jitter buffer latency: {self.jitterbuffer_latency} ms")
        if self.enable_latency_tracer:
            print("Latency tracer enabled (check stderr for latency measurements)")
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
    
    def stop(self):
        """Stop the pipeline gracefully."""
        # Close TCP client
        self._close_tcp_client()

        if self.pipeline:
            print("Stopping pipeline...")
            # Send EOS to ensure proper cleanup (required for mp4mux to write moov atom)
            self.pipeline.send_event(Gst.Event.new_eos())

            # Wait for EOS to propagate through the pipeline
            # This is critical for MP4 muxer to finalize the file
            if self.enable_recording:
                print("Waiting for video file to finalize...")
                bus = self.pipeline.get_bus()
                # Wait up to 5 seconds for EOS message
                msg = bus.timed_pop_filtered(5 * Gst.SECOND, Gst.MessageType.EOS | Gst.MessageType.ERROR)
                if msg:
                    if msg.type == Gst.MessageType.EOS:
                        print("EOS received, video file finalized.")
                        if self.output_dir:
                            print(f"Video saved to: {self.output_dir}")
                    elif msg.type == Gst.MessageType.ERROR:
                        err, debug = msg.parse_error()
                        print(f"Error during finalization: {err.message}")
                        if debug:
                            print(f"Debug info: {debug}")
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
        description="GStreamer Video Receiver - Receive and display video via UDP/RTP"
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
    
    # Extract receiver-specific and common parameters
    receiver_config = config.get("receiver", {})
    common_config = config.get("common", {})
    # Get receiver-specific logging config (nested under receiver)
    receiver_logging_config = receiver_config.get("logging", {})

    receiver = GStreamerReceiver(
        port=common_config.get("udp_port", 5004),
        jitterbuffer_latency=receiver_config.get("jitterbuffer_latency", 0),
        enable_latency_tracer=receiver_config.get("enable_latency_tracer", True),
        enable_recording=receiver_logging_config.get("enable_video_recording", False),
        output_dir=receiver_logging_config.get("output_dir", None),
        video_filename_prefix=receiver_logging_config.get("video_filename_prefix", "receiver_video")
    )

    # Set sender host for RTT echo (required for RTT measurement)
    receiver.sender_host = receiver_config.get("sender_ip", None)
    if receiver.sender_host:
        print(f"RTT echo enabled, will connect to sender at {receiver.sender_host}:{receiver.rtt_port}")
    else:
        print("RTT echo disabled (no sender_ip in config)")
    
    receiver.start()


if __name__ == "__main__":
    main()
