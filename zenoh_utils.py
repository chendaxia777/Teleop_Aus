from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

import zenoh


DEFAULT_PROTOCOL = "tcp"
DEFAULT_PORT = 7447


class ZenohPubSubClient:
    """Small reusable wrapper for common Zenoh publisher/subscriber scripts."""

    def __init__(
        self,
        *,
        endpoints: list[str] | tuple[str, ...] | None = None,
        config_path: str | Path | None = None,
        config: zenoh.Config | None = None,
        init_log: bool = True,
        log_level: str = "error",
    ):
        self.endpoints = list(endpoints) if endpoints is not None else None
        self.config_path = Path(config_path) if config_path is not None else None
        self.config = config
        self.init_log = init_log
        self.log_level = log_level

        self.session = None
        self.publishers = {}
        self.subscribers = {}

    @staticmethod
    def endpoint_from_json_config(config_path: str | Path) -> str:
        path = Path(config_path)
        with path.open("r", encoding="utf-8") as config_file:
            data = json.load(config_file)

        protocol = data.get("protocol", DEFAULT_PROTOCOL)
        ip_address = data["router_ip_address"]
        port = data.get("port", DEFAULT_PORT)
        return f"{protocol}/{ip_address}:{port}"

    @classmethod
    def from_json_config(
        cls,
        config_path: str | Path,
        *,
        init_log: bool = True,
        log_level: str = "error",
    ) -> "ZenohPubSubClient":
        return cls(
            endpoints=[cls.endpoint_from_json_config(config_path)],
            init_log=init_log,
            log_level=log_level,
        )

    def build_config(self) -> zenoh.Config:
        if self.config is not None:
            return self.config

        conf = zenoh.Config()
        endpoints = self.endpoints
        if endpoints is None and self.config_path is not None:
            endpoints = [self.endpoint_from_json_config(self.config_path)]
        if endpoints:
            conf.insert_json5("connect/endpoints", json.dumps(list(endpoints)))
        return conf

    def open(self) -> "ZenohPubSubClient":
        if self.session is not None:
            return self

        if self.init_log:
            zenoh.init_log_from_env_or(self.log_level)

        self.session = zenoh.open(self.build_config())
        return self

    def close(self):
        self.subscribers.clear()
        self.publishers.clear()
        if self.session is not None:
            self.session.close()
            self.session = None

    def __enter__(self) -> "ZenohPubSubClient":
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    def _require_session(self):
        if self.session is None:
            raise RuntimeError("Zenoh session is not open. Call open() first.")
        return self.session

    def declare_publisher(self, key: str):
        if key not in self.publishers:
            self.publishers[key] = self._require_session().declare_publisher(key)
        return self.publishers[key]

    def declare_subscriber(self, key: str, callback: Callable):
        if key in self.subscribers:
            raise ValueError(f"Subscriber already declared for key: {key}")
        subscriber = self._require_session().declare_subscriber(key, callback)
        self.subscribers[key] = subscriber
        return subscriber

    def publish(self, key: str, payload):
        publisher = self.declare_publisher(key)
        publisher.put(payload)
