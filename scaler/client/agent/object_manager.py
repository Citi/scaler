import logging
import threading
from typing import Optional, Set

from scaler.client.agent.mixins import ObjectManager, FutureManager
from scaler.io.async_connector import AsyncConnector
from scaler.io.async_object_storage_connector import AsyncObjectStorageConnector
from scaler.io.utility import concat_list_of_bytes
from scaler.protocol.python.common import ObjectContent
from scaler.protocol.python.message import ObjectInstruction, ObjectRequest, TaskResult
from scaler.utility.object_storage_config import ObjectStorageConfig


class ClientObjectManager(ObjectManager):
    def __init__(self):
        self._sent_object_ids: Set[bytes] = set()
        self._sent_serializer_id: Optional[bytes] = None
        self._task_result_ids: Set[bytes] = set()

        self._connector_internal: Optional[AsyncConnector] = None
        self._connector_object_storage: Optional[AsyncObjectStorageConnector] = None
        self._future_manager: Optional[FutureManager] = None

        self._is_ready_event = threading.Event()

    def register(self, connector_internal: AsyncConnector, future_manager: FutureManager):
        self._connector_internal = connector_internal
        self._future_manager = future_manager

    def ready(self) -> bool:
        return self._is_ready_event.is_set()

    def wait_until_ready(self) -> None:
        self._is_ready_event.wait()

    async def connect_to_object_storage(self, object_storage_config: ObjectStorageConfig):
        assert self._connector_object_storage is None

        self._connector_object_storage = AsyncObjectStorageConnector(
            object_storage_config.host,
            object_storage_config.port,
            self.on_object_storage_get_response,
        )
        await self._connector_object_storage.connect()

        self._is_ready_event.set()

    async def on_object_instruction(self, instruction: ObjectInstruction):
        if instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Create:
            await self.__on_object_instruction_create(instruction)
        elif instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Delete:
            await self.__on_object_instruction_delete(instruction)
        elif instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Clear:
            await self.clear_all_objects(clear_serializer=False)

    async def on_object_request(self, message: ObjectRequest):
        for object_id in message.object_ids:
            await self._connector_object_storage.send_get_request(object_id)

    async def on_object_storage_get_response(self, object_id: bytes, payload: bytes):
        if object_id not in self._task_result_ids:
            # This might be a requested task result for a no longer known future.
            logging.warning(f"unexpected object get response for object_id={object_id.hex()}.")
            return

        self._future_manager.on_object_storage_get_response(object_id, payload)

        # Immediately request the result object to be removed.
        self._task_result_ids.remove(object_id)
        await self._connector_object_storage.send_delete_request(object_id)

    def on_task_result(self, task_result: TaskResult):
        # TODO: if the future has been cancelled, we should immediately request the object for deletion.
        self._task_result_ids.update(task_result.results)

    async def clear_all_objects(self, clear_serializer):
        cleared_object_ids = self._sent_object_ids.union(self._task_result_ids)

        if not clear_serializer and self._sent_serializer_id is not None:
            self._sent_object_ids = {self._sent_serializer_id}
            cleared_object_ids.remove(self._sent_serializer_id)
        else:
            self._sent_object_ids = set()
            self._sent_serializer_id = None

        self._task_result_ids.clear()

        for object_id in cleared_object_ids:
            await self._connector_object_storage.send_delete_request(object_id)

    async def __on_object_instruction_create(self, instruction: ObjectInstruction):
        assert instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Create

        new_object_ids = set(instruction.object_content.object_ids) - self._sent_object_ids
        if not new_object_ids:
            return

        if ObjectContent.ObjectContentType.Serializer in instruction.object_content.object_types:
            if self._sent_serializer_id is not None:
                raise ValueError("trying to send multiple serializers.")

            serializer_index = instruction.object_content.object_types.index(ObjectContent.ObjectContentType.Serializer)
            self._sent_serializer_id = instruction.object_content.object_ids[serializer_index]

        packed_objects = zip(instruction.object_content.object_ids, instruction.object_content.object_bytes)

        for object_id, object_bytes in packed_objects:
            if object_id not in new_object_ids:
                continue

            await self._connector_object_storage.send_set_request(object_id, concat_list_of_bytes(object_bytes))

    async def __on_object_instruction_delete(self, instruction: ObjectInstruction):
        assert instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Delete

        if self._sent_serializer_id in instruction.object_content.object_ids:
            raise ValueError("trying to delete serializer.")

        self._sent_object_ids.difference_update(instruction.object_content.object_ids)
        self._task_result_ids.difference_update(instruction.object_content.object_ids)

        for object_id in instruction.object_content.object_ids:
            await self._connector_object_storage.send_delete_request(object_id)
