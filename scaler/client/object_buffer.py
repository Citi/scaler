import dataclasses
import pickle
from typing import Any, Callable, List, Optional, Set, Tuple

import cloudpickle

from scaler.client.serializer.mixins import Serializer
from scaler.io.sync_connector import SyncConnector
from scaler.io.sync_object_storage_connector import SyncObjectStorageConnector
from scaler.protocol.python.common import ObjectMetadata
from scaler.protocol.python.message import ObjectInstruction
from scaler.utility.object_utility import generate_object_id, generate_serializer_object_id


@dataclasses.dataclass
class ObjectCache:
    object_id: bytes
    object_type: ObjectMetadata.ObjectContentType
    object_name: bytes
    object_size: int


class ObjectBuffer:
    def __init__(
        self,
        identity: bytes,
        serializer: Serializer,
        connector_agent: SyncConnector,
        connector_storage: SyncObjectStorageConnector,
    ):
        self._identity = identity
        self._serializer = serializer

        self._connector_agent = connector_agent
        self._connector_storage = connector_storage

        self._pending_objects: List[ObjectCache] = list()
        self._pending_delete_objects: Set[bytes] = set()

    def buffer_send_serializer(self) -> ObjectCache:
        serializer_cache, serializer_payload = self.__construct_serializer()

        is_overridden = self._connector_storage.set_object(serializer_cache.object_id, serializer_payload)
        assert not is_overridden

        self._pending_objects.append(serializer_cache)
        return serializer_cache

    def buffer_send_function(self, fn: Callable) -> ObjectCache:
        function_cache, function_payload = self.__construct_function(fn)

        self._connector_storage.set_object(function_cache.object_id, function_payload)

        self._pending_objects.append(function_cache)
        return function_cache

    def buffer_send_object(self, obj: Any, name: Optional[str] = None) -> ObjectCache:
        object_cache, object_payload = self.__construct_object(obj, name)

        self._connector_storage.set_object(object_cache.object_id, object_payload)

        self._pending_objects.append(object_cache)
        return object_cache

    def buffer_delete_object(self, object_ids: Set[bytes]):
        for object_id in object_ids:
            object_found = self._connector_storage.delete_object(object_id)
            assert object_found

        self._pending_delete_objects.update(object_ids)

    def commit_send_objects(self):
        if not self._pending_objects:
            return

        objects_to_send = [
            (obj_cache.object_id, obj_cache.object_type, obj_cache.object_name)
            for obj_cache in self._pending_objects
        ]

        self._connector_agent.send(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Create,
                self._identity,
                ObjectMetadata.new_msg(*zip(*objects_to_send)),
            )
        )

        self._pending_objects.clear()

    def commit_delete_objects(self):
        if not self._pending_delete_objects:
            return

        self._connector_agent.send(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Delete,
                self._identity,
                ObjectMetadata.new_msg(tuple(self._pending_delete_objects)),
            )
        )

        self._pending_delete_objects.clear()

    def clear(self):
        """
        remove all committed and pending objects.
        """

        self._pending_delete_objects.clear()
        self._pending_objects.clear()

        self._connector_agent.send(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Clear, self._identity, ObjectMetadata.new_msg(tuple())
            )
        )

    def __construct_serializer(self) -> Tuple[ObjectCache, bytes]:
        serializer_payload = cloudpickle.dumps(self._serializer, protocol=pickle.HIGHEST_PROTOCOL)
        object_id = generate_serializer_object_id(self._identity)
        serializer_cache = ObjectCache(
            object_id,
            ObjectMetadata.ObjectContentType.Serializer,
            b"serializer",
            len(serializer_payload)
        )

        return serializer_cache, serializer_payload

    def __construct_function(self, fn: Callable) -> Tuple[ObjectCache, bytes]:
        function_payload = self._serializer.serialize(fn)
        object_id = generate_object_id(self._identity, function_payload)
        function_cache = ObjectCache(
            object_id,
            ObjectMetadata.ObjectContentType.Object,
            getattr(fn, "__name__", f"<func {object_id.hex()[:6]}>").encode(),
            len(function_payload),
        )

        return function_cache, function_payload

    def __construct_object(self, obj: Any, name: Optional[str] = None) -> Tuple[ObjectCache, bytes]:
        object_payload = self._serializer.serialize(obj)
        object_id = generate_object_id(self._identity, object_payload)
        name_bytes = name.encode() if name else f"<obj {object_id.hex()[-6:]}>".encode()
        object_cache = ObjectCache(object_id, ObjectMetadata.ObjectContentType.Object, name_bytes, len(object_payload))

        return object_cache, object_payload
