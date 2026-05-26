from __future__ import annotations

import ctypes
import itertools
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import zenoh

from zenoh_utils import ZenohPubSubClient


DEFAULT_KEY = "example/command"
DEFAULT_ECHO_KEY = "example/command/echo"
DEFAULT_SERVER_CONFIG = Path(__file__).with_name("z_server_config.json")
FRAME_INTERVAL = 1 / 30

VK_SHIFT = 0x10
VK_CONTROL = 0x11
VK_ESCAPE = 0x1B
VK_SPACE = 0x20
VK_LEFT = 0x25
VK_UP = 0x26
VK_RIGHT = 0x27
VK_DOWN = 0x28

KEYS = (
    ("w", ord("W")),
    ("a", ord("A")),
    ("s", ord("S")),
    ("d", ord("D")),
    ("q", ord("Q")),
    ("e", ord("E")),
    ("up", VK_UP),
    ("down", VK_DOWN),
    ("left", VK_LEFT),
    ("right", VK_RIGHT),
    ("space", VK_SPACE),
    ("shift", VK_SHIFT),
    ("ctrl", VK_CONTROL),
    ("esc", VK_ESCAPE),
)


def parse_echo(payload: str) -> tuple[int, int]:
    fields = {}
    for part in payload.split(";"):
        name, value = part.split("=", 1)
        fields[name] = value

    return int(fields["seq"]), int(fields["timestamp_ns"])


def is_key_down(virtual_key: int) -> bool:
    return bool(ctypes.windll.user32.GetAsyncKeyState(virtual_key) & 0x8000)


def get_held_keys() -> list[str]:
    return [name for name, virtual_key in KEYS if is_key_down(virtual_key)]


def main():
    print("Opening session...")
    with ZenohPubSubClient.from_json_config(DEFAULT_SERVER_CONFIG) as client:
        print(f"Declaring Publisher on '{DEFAULT_KEY}'...")
        client.declare_publisher(DEFAULT_KEY)

        print(f"Declaring Subscriber on '{DEFAULT_ECHO_KEY}'...")

        def listener(sample: zenoh.Sample):
            echoed_payload = sample.payload.to_string()
            try:
                seq, timestamp_ns = parse_echo(echoed_payload)
            except (KeyError, ValueError) as exc:
                print(f"Malformed echo payload '{echoed_payload}': {exc}")
                return

            rtt_ms = (time.perf_counter_ns() - timestamp_ns) / 1_000_000
            print(f"<< Echo seq={seq} rtt={rtt_ms:.3f} ms")

        client.declare_subscriber(DEFAULT_ECHO_KEY, listener)

        print("Hold a configured key to send at 30 FPS. Press ESC to quit.")
        for seq in itertools.count():
            frame_start = time.perf_counter()
            held_keys = get_held_keys()

            if "esc" in held_keys:
                print("ESC pressed, quitting...")
                break

            if held_keys:
                timestamp_ns = time.perf_counter_ns()
                key_payload = "+".join(held_keys)
                payload = f"seq={seq};timestamp_ns={timestamp_ns};payload={key_payload}"
                print(f"Putting Data '{payload}')...")
                client.publish(DEFAULT_KEY, payload)

            elapsed = time.perf_counter() - frame_start
            time.sleep(max(0, FRAME_INTERVAL - elapsed))


if __name__ == "__main__":
    main()
