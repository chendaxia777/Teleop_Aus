import json
import time
from pathlib import Path

import zenoh

DEFAULT_KEY = "example/command"
DEFAULT_ECHO_KEY = "example/command/echo"
DEFAULT_PROTOCOL = "tcp"
DEFAULT_PORT = 7447
DEFAULT_SERVER_CONFIG = Path(__file__).with_name("z_server_config.json")


def get_connect_endpoint_from_config(config_path: Path = DEFAULT_SERVER_CONFIG) -> str:
    with config_path.open("r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    protocol = config.get("protocol", DEFAULT_PROTOCOL)
    ip_address = config["router_ip_address"]
    port = config.get("port", DEFAULT_PORT)
    return f"{protocol}/{ip_address}:{port}"


def get_config() -> zenoh.Config:
    conf = zenoh.Config()
    conf.insert_json5("connect/endpoints", json.dumps([get_connect_endpoint_from_config()]))
    return conf


def parse_command(payload: str) -> tuple[int, int]:
    fields = {}
    for part in payload.split(";"):
        name, value = part.split("=", 1)
        fields[name] = value

    return int(fields["seq"]), int(fields["timestamp_ns"])


def main():
    zenoh.init_log_from_env_or("error")

    conf = get_config()

    print("Opening session...")
    with zenoh.open(conf) as session:
        print(f"Declaring Subscriber on '{DEFAULT_KEY}'...")
        print(f"Declaring Publisher on '{DEFAULT_ECHO_KEY}'...")
        pub = session.declare_publisher(DEFAULT_ECHO_KEY)

        def listener(sample: zenoh.Sample):
            payload = sample.payload.to_string()
            print(
                f">> [Subscriber] Received ('{payload}')"
            )
            try:
                seq, timestamp_ns = parse_command(payload)
            except (KeyError, ValueError) as exc:
                print(f"Malformed command payload '{payload}': {exc}")
                return

            echo_payload = f"seq={seq};timestamp_ns={timestamp_ns}"
            # print(f"<< [Publisher] Echoing ('{echo_payload}')")
            pub.put(echo_payload)

        session.declare_subscriber(DEFAULT_KEY, listener)

        print("Press CTRL-C to quit...")
        while True:
            time.sleep(1)


if __name__ == "__main__":
    main()