#
# Copyright (c) 2022 ZettaScale Technology
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
# which is available at https://www.apache.org/licenses/LICENSE-2.0.
#
# SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
#
# Contributors:
#   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
#
import time
import json
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


def parse_command(payload: str) -> tuple[int, int]:
    fields = {}
    for part in payload.split(";", 2)[:2]:
        name, value = part.split("=", 1)
        fields[name] = value

    return int(fields["seq"]), int(fields["timestamp_ns"])


def main(conf: zenoh.Config, key: str, echo_key: str):
    # initiate logging
    zenoh.init_log_from_env_or("error")

    print("Opening session...")
    with zenoh.open(conf) as session:
        print(f"Declaring Subscriber on '{key}'...")
        print(f"Declaring Publisher on '{echo_key}'...")
        pub = session.declare_publisher(echo_key)

        def listener(sample: zenoh.Sample):
            payload = sample.payload.to_string()
            print(
                f">> [Subscriber] Received {sample.kind} ('{sample.key_expr}': '{payload}')"
            )
            try:
                seq, timestamp_ns = parse_command(payload)
            except (KeyError, ValueError) as exc:
                print(f"Malformed command payload '{payload}': {exc}")
                return

            echo_payload = f"seq={seq};timestamp_ns={timestamp_ns}"
            # print(f"<< [Publisher] Echoing ('{echo_key}': '{echo_payload}')")
            pub.put(echo_payload)

        session.declare_subscriber(key, listener)

        print("Press CTRL-C to quit...")
        while True:
            time.sleep(1)


# --- Command line argument parsing --- --- --- --- --- ---
if __name__ == "__main__":
    import argparse

    import common

    parser = argparse.ArgumentParser(prog="z_sub", description="zenoh sub example")
    common.add_config_arguments(parser)
    parser.add_argument(
        "--key",
        "-k",
        dest="key",
        default=DEFAULT_KEY,
        type=str,
        help="The key expression to subscribe to.",
    )
    parser.add_argument(
        "--echo-key",
        dest="echo_key",
        default=DEFAULT_ECHO_KEY,
        type=str,
        help="The key expression to publish echoed timestamps onto.",
    )

    args = parser.parse_args()
    if args.connect is None:
        args.connect = [get_connect_endpoint_from_config()]

    conf = common.get_config_from_args(args)

    main(conf, args.key, args.echo_key)
