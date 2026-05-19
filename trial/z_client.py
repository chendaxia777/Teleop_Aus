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
import itertools
import time
from typing import Optional

import zenoh

DEFAULT_KEY = "example/command"
DEFAULT_ECHO_KEY = "example/command/echo"


def parse_echo(payload: str) -> tuple[int, int]:
    fields = {}
    for part in payload.split(";"):
        name, value = part.split("=", 1)
        fields[name] = value

    return int(fields["seq"]), int(fields["timestamp_ns"])


def main(
    conf: zenoh.Config,
    key: str,
    echo_key: str,
    payload: str,
    iter: Optional[int],
    interval: int,
    add_matching_listener: bool,
):
    # initiate logging
    zenoh.init_log_from_env_or("error")

    print("Opening session...")
    with zenoh.open(conf) as session:
        print(f"Declaring Publisher on '{key}'...")
        pub = session.declare_publisher(key)

        print(f"Declaring Subscriber on '{echo_key}'...")

        def listener(sample: zenoh.Sample):
            echoed_payload = sample.payload.to_string()
            try:
                seq, timestamp_ns = parse_echo(echoed_payload)
            except (KeyError, ValueError) as exc:
                print(f"Malformed echo payload '{echoed_payload}': {exc}")
                return

            rtt_ms = (time.perf_counter_ns() - timestamp_ns) / 1_000_000
            print(f"<< Echo seq={seq} rtt={rtt_ms:.3f} ms")

        _sub = session.declare_subscriber(echo_key, listener)

        if add_matching_listener:

            def on_matching_status_update(status: zenoh.MatchingStatus):
                if status.matching:
                    print("Publisher has matching subscribers.")
                else:
                    print("Publisher has NO MORE matching subscribers")

            pub.declare_matching_listener(on_matching_status_update)

        print("Press CTRL-C to quit...")
        for idx in itertools.count() if iter is None else range(iter):
            time.sleep(interval)
            timestamp_ns = time.perf_counter_ns()
            buf = f"seq={idx};timestamp_ns={timestamp_ns};payload={payload}"
            # print(f"Putting Data ('{key}': '{buf}')...")
            pub.put(buf)

        if iter is not None:
            time.sleep(interval)


# --- Command line argument parsing --- --- --- --- --- ---
if __name__ == "__main__":
    import argparse

    import common

    parser = argparse.ArgumentParser(prog="z_pub", description="zenoh pub example")
    common.add_config_arguments(parser)
    parser.add_argument(
        "--key",
        "-k",
        dest="key",
        default=DEFAULT_KEY,
        type=str,
        help="The key expression to publish onto.",
    )
    parser.add_argument(
        "--echo-key",
        dest="echo_key",
        default=DEFAULT_ECHO_KEY,
        type=str,
        help="The key expression to subscribe to for echoed timestamps.",
    )
    parser.add_argument(
        "--payload",
        "-p",
        dest="payload",
        default="Pub from Python!",
        type=str,
        help="The payload to publish.",
    )
    parser.add_argument(
        "--iter", dest="iter", type=int, help="How many puts to perform"
    )
    parser.add_argument(
        "--interval",
        dest="interval",
        type=float,
        default=1.0,
        help="Interval between each put",
    )
    parser.add_argument(
        "--add-matching-listener",
        default=False,
        action="store_true",
        help="Add matching listener",
    )

    args = parser.parse_args()
    conf = common.get_config_from_args(args)

    main(
        conf,
        args.key,
        args.echo_key,
        args.payload,
        args.iter,
        args.interval,
        args.add_matching_listener,
    )
