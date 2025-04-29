import time
from typing import Optional

import psutil

from scaler.client.agent.mixins import HeartbeatManager, ObjectManager
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.message import ClientHeartbeat, ClientHeartbeatEcho
from scaler.protocol.python.status import Resource
from scaler.utility.mixins import Looper
from scaler.utility.object_storage_config import ObjectStorageConfig


class ClientHeartbeatManager(Looper, HeartbeatManager):
    def __init__(self, death_timeout_seconds: int):
        self._death_timeout_seconds = death_timeout_seconds

        self._process = psutil.Process()

        self._last_scheduler_contact = time.time()
        self._start_timestamp_ns = 0
        self._latency_us = 0
        self._connected = False

        self._connector_external: Optional[AsyncConnector] = None
        self._object_manager: Optional[ObjectManager] = None

    def register(self, connector_external: AsyncConnector, object_manager: ObjectManager):
        self._connector_external = connector_external
        self._object_manager = object_manager

    async def send_heartbeat(self):
        await self._connector_external.send(
            ClientHeartbeat.new_msg(
                Resource.new_msg(int(self._process.cpu_percent() * 10), self._process.memory_info().rss),
                self._latency_us,
            )
        )

    async def on_heartbeat_echo(self, heartbeat: ClientHeartbeatEcho):
        if not self._connected:
            self._connected = True

        self._last_scheduler_contact = time.time()
        if self._start_timestamp_ns == 0:
            # not handling echo if we didn't send out heartbeat
            return

        self._latency_us = int(((time.time_ns() - self._start_timestamp_ns) / 2) // 1_000)
        self._start_timestamp_ns = 0

        if self._object_manager.ready():
            return

        object_storage_address = heartbeat.object_storage_address()
        await self._object_manager.connect_to_object_storage(object_storage_address.host, object_storage_address.port)

    async def routine(self):
        if time.time() - self._last_scheduler_contact > self._death_timeout_seconds:
            raise TimeoutError(
                f"Timeout when connecting to scheduler {self._connector_external.address} "
                f"in {self._death_timeout_seconds} seconds"
            )

        if self._start_timestamp_ns != 0:
            # already sent heartbeat, expecting heartbeat echo, so not sending
            return

        await self.send_heartbeat()
        self._start_timestamp_ns = time.time_ns()
