from __future__ import annotations

import itertools
import json
import random
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import zenoh

from zenoh_utils import ZenohPubSubClient


DEFAULT_KEY = "example/random_array"
DEFAULT_ECHO_KEY = "example/random_array/echo"
DEFAULT_SERVER_CONFIG = Path(__file__).with_name("z_server_config.json")
ARRAY_LENGTH = 10
FRAME_INTERVAL = 1 / 30
RANDOM_MIN = 0
RANDOM_MAX = 100


def make_payload(seq: int) -> str:
    return json.dumps(
        {
            "seq": seq,
            "timestamp_ns": time.perf_counter_ns(),
            "numbers": [
                random.randint(RANDOM_MIN, RANDOM_MAX) for _ in range(ARRAY_LENGTH)
            ],
        },
        separators=(",", ":"),
    )


def parse_echo(payload: str) -> tuple[int, int, list[int]]:
    data = json.loads(payload)
    return int(data["seq"]), int(data["timestamp_ns"]), list(data["numbers"])


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
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                print(f"Malformed echo payload '{echoed_payload}': {exc}")
                return

            rtt_ms = (time.perf_counter_ns() - timestamp_ns) / 1_000_000
            print(f"<< Echo seq={seq} rtt={rtt_ms:.3f} ms")

        client.declare_subscriber(DEFAULT_ECHO_KEY, listener)

        print("Sending random number arrays at 30 FPS. Press CTRL-C to quit...")
        for seq in itertools.count():
            frame_start = time.perf_counter()
            payload = make_payload(seq)
            print(f">> Publishing '{payload}'")
            client.publish(DEFAULT_KEY, payload)

            elapsed = time.perf_counter() - frame_start
            time.sleep(max(0, FRAME_INTERVAL - elapsed))


if __name__ == "__main__":
    main()
