from __future__ import annotations

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


def parse_command(payload: str) -> tuple[int, int]:
    fields = {}
    for part in payload.split(";"):
        name, value = part.split("=", 1)
        fields[name] = value

    return int(fields["seq"]), int(fields["timestamp_ns"])


def main():
    print("Opening session...")
    with ZenohPubSubClient.from_json_config(DEFAULT_SERVER_CONFIG) as client:
        print(f"Declaring Subscriber on '{DEFAULT_KEY}'...")
        print(f"Declaring Publisher on '{DEFAULT_ECHO_KEY}'...")
        client.declare_publisher(DEFAULT_ECHO_KEY)

        def listener(sample: zenoh.Sample):
            payload = sample.payload.to_string()
            print(f">> [Subscriber] Received ('{payload}')")
            try:
                seq, timestamp_ns = parse_command(payload)
            except (KeyError, ValueError) as exc:
                print(f"Malformed command payload '{payload}': {exc}")
                return

            echo_payload = f"seq={seq};timestamp_ns={timestamp_ns}"
            client.publish(DEFAULT_ECHO_KEY, echo_payload)

        client.declare_subscriber(DEFAULT_KEY, listener)

        print("Press CTRL-C to quit...")
        while True:
            time.sleep(1)


if __name__ == "__main__":
    main()
