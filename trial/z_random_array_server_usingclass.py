from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import zenoh

from zenoh_utils import ZenohPubSubClient
from zenoh_utils import make_json_echo_payload


DEFAULT_KEY = "example/random_array"
DEFAULT_ECHO_KEY = "example/random_array/echo"
DEFAULT_SERVER_CONFIG = Path(__file__).with_name("z_server_config.json")


def parse_random_array(payload: str) -> tuple[int, int, list[int]]:
    data = json.loads(payload)
    return int(data["seq"]), int(data["timestamp_ns"]), list(data["numbers"])


def main():
    print("Opening session...")
    with ZenohPubSubClient.from_json_config(DEFAULT_SERVER_CONFIG) as server:
        print(f"Declaring Subscriber on '{DEFAULT_KEY}'...")
        print(f"Declaring Publisher on '{DEFAULT_ECHO_KEY}'...")
        server.declare_publisher(DEFAULT_ECHO_KEY)

        def listener(sample: zenoh.Sample):
            payload = sample.payload.to_string()
            try:
                seq, timestamp_ns, numbers = parse_random_array(payload)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                print(f"Malformed random array payload '{payload}': {exc}")
                return

            print(f">> Received seq={seq} numbers={numbers}")
            echo_payload = make_json_echo_payload(seq, timestamp_ns)
            server.publish(DEFAULT_ECHO_KEY, echo_payload)

        server.declare_subscriber(DEFAULT_KEY, listener)

        print("Press CTRL-C to quit...")
        while True:
            time.sleep(1)


if __name__ == "__main__":
    main()
