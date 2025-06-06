import ctypes
import gc
import logging
import multiprocessing
import platform
import threading
import time
from typing import Any, Dict, Optional

import cloudpickle
import psutil

from scaler.client.serializer.mixins import Serializer
from scaler.io.config import CLEANUP_INTERVAL_SECONDS
from scaler.utility.exceptions import DeserializeObjectError
from scaler.utility.object_id import ObjectID


class ObjectCache(threading.Thread):
    def __init__(self, garbage_collect_interval_seconds: int, trim_memory_threshold_bytes: int):
        threading.Thread.__init__(self)

        self._serializers: Dict[bytes, Serializer] = dict()

        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._previous_garbage_collect_time = time.time()
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes

        self._cached_objects: Dict[ObjectID, Any] = {}
        self._cached_objects_alive_since: Dict[ObjectID, float] = dict()
        self._process = psutil.Process(multiprocessing.current_process().pid)
        self._libc = ctypes.cdll.LoadLibrary("libc.{}".format("so.6" if platform.uname()[0] != "Darwin" else "dylib"))

        self._stop_event = threading.Event()

    def run(self) -> None:
        try:
            while not self._stop_event.wait(timeout=CLEANUP_INTERVAL_SECONDS):
                self.__clean_memory()
        finally:
            self.__clear()  # gracefully destroy all cached objects

    def destroy(self) -> None:
        self._stop_event.set()

    def add_serializer(self, client: bytes, serializer: Serializer):
        self._serializers[client] = serializer

    def serialize(self, client: bytes, obj: Any) -> bytes:
        return self.get_serializer(client).serialize(obj)

    def deserialize(self, client: bytes, payload: bytes) -> Any:
        return self.get_serializer(client).deserialize(payload)

    def add_object(self, client: bytes, object_id: ObjectID, object_bytes: bytes) -> None:

        if object_id.is_serializer():
            self.add_serializer(client, cloudpickle.loads(object_bytes))
        else:
            try:
                deserialized = self.deserialize(client, object_bytes)
            except Exception:  # noqa
                logging.exception(f"failed to deserialize received {object_id!r}, length={len(object_bytes)}")

            self._cached_objects[object_id] = deserialized
            self._cached_objects_alive_since[object_id] = time.time()

    def del_object(self, object_id: ObjectID):
        self._cached_objects_alive_since.pop(object_id, None)
        self._cached_objects.pop(object_id, None)

    def has_object(self, object_id: ObjectID):
        return object_id in self._cached_objects or object_id in self._serializers

    def get_object(self, object_id: ObjectID) -> Optional[Any]:
        if object_id not in self._cached_objects:
            raise ValueError(f"cannot get object for {object_id!r}")

        obj = self._cached_objects[object_id]

        self._cached_objects_alive_since[object_id] = time.time()
        return obj

    def get_serializer(self, client: bytes) -> Serializer:
        serializer = self._serializers.get(client)

        if serializer is None:
            raise DeserializeObjectError(f"cannot get serializer for client={client.hex()}")

        return serializer

    def __clean_memory(self):
        if time.time() - self._previous_garbage_collect_time < self._garbage_collect_interval_seconds:
            return

        self._previous_garbage_collect_time = time.time()

        gc.collect()

        if self._process.memory_info().rss < self._trim_memory_threshold_bytes:
            return

        self._libc.malloc_trim(0)

    def __clear(self) -> None:
        self._cached_objects.clear()
        self._cached_objects_alive_since.clear()
