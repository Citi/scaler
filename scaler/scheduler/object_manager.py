import dataclasses
import logging
from asyncio import Queue
from typing import Optional, Set

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.io.async_object_storage_connector import AsyncObjectStorageConnector
from scaler.protocol.python.common import ObjectMetadata
from scaler.protocol.python.message import ObjectInstruction
from scaler.protocol.python.status import ObjectManagerStatus
from scaler.scheduler.mixins import ClientManager, ObjectManager, WorkerManager
from scaler.scheduler.object_usage.object_tracker import ObjectTracker, ObjectUsage
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.identifiers import ClientID, ObjectID


@dataclasses.dataclass
class _ObjectCreation(ObjectUsage):
    object_id: ObjectID
    object_creator: ClientID
    object_type: ObjectMetadata.ObjectContentType
    object_name: bytes

    def get_object_key(self) -> ObjectID:
        return self.object_id


class VanillaObjectManager(ObjectManager, Looper, Reporter):
    def __init__(self):
        self._object_tracker: ObjectTracker[ClientID, ObjectID, _ObjectCreation] = ObjectTracker(
            "object_usage", self.__finished_object_storage
        )

        self._queue_deleted_object_ids: Queue[ObjectID] = Queue()

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None

        self._client_manager: Optional[ClientManager] = None
        self._worker_manager: Optional[WorkerManager] = None

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        client_manager: ClientManager,
        worker_manager: WorkerManager,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._connector_storage = connector_storage
        self._client_manager = client_manager
        self._worker_manager = worker_manager

    async def on_object_instruction(self, source: bytes, instruction: ObjectInstruction):
        if instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Create:
            self.__on_object_create(source, instruction)
            return

        if instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Delete:
            self.on_del_objects(instruction.object_user, set(instruction.object_metadata.object_ids))
            return

        logging.error(
            f"received unknown object instruction_type={instruction.instruction_type} from {source=}"
        )

    def on_add_object(
        self,
        client_id: ClientID,
        object_id: ObjectID,
        object_type: ObjectMetadata.ObjectContentType,
        object_name: bytes,
    ):
        creation = _ObjectCreation(object_id, client_id, object_type, object_name)
        logging.debug(
            f"add object cache "
            f"object_name={creation.object_name!r}, "
            f"object_type={creation.object_type}, "
            f"object_id={creation.object_id!r}"
        )

        self._object_tracker.add_object(creation)
        self._object_tracker.add_blocks_for_one_object(creation.get_object_key(), {creation.object_creator})

    def on_del_objects(self, client_id: ClientID, object_ids: Set[ObjectID]):
        for object_id in object_ids:
            self._object_tracker.remove_one_block_for_objects({object_id}, client_id)

    def clean_client(self, client_id: ClientID):
        self._object_tracker.remove_blocks({client_id})

    async def routine(self):
        await self.__routine_send_objects_deletions()

    def has_object(self, object_id: ObjectID) -> bool:
        return self._object_tracker.has_object(object_id)

    def get_object_name(self, object_id: ObjectID) -> bytes:
        if not self.has_object(object_id):
            return b"<Unknown>"

        return self._object_tracker.get_object(object_id).object_name

    def get_status(self) -> ObjectManagerStatus:
        return ObjectManagerStatus.new_msg(self._object_tracker.object_count())

    async def __routine_send_objects_deletions(self):
        deleted_object_ids = [await self._queue_deleted_object_ids.get()]
        self._queue_deleted_object_ids.task_done()

        while not self._queue_deleted_object_ids.empty():
            deleted_object_ids.append(self._queue_deleted_object_ids.get_nowait())
            self._queue_deleted_object_ids.task_done()

        for worker in self._worker_manager.get_worker_ids():
            await self._binder.send(
                worker,
                ObjectInstruction.new_msg(
                    ObjectInstruction.ObjectInstructionType.Delete,
                    None,
                    ObjectMetadata.new_msg(tuple(deleted_object_ids)),
                ),
            )

        for object_id in deleted_object_ids:
            await self._connector_storage.delete_object(object_id)

    def __on_object_create(self, source: bytes, instruction: ObjectInstruction):
        if not self._client_manager.has_client_id(instruction.object_user):
            logging.error(f"received object creation from {source!r} for unknown client {instruction.object_user!r}")
            return

        for object_id, object_type, object_name in zip(
            instruction.object_metadata.object_ids,
            instruction.object_metadata.object_types,
            instruction.object_metadata.object_names,
        ):
            self.on_add_object(instruction.object_user, object_id, object_type, object_name)

    def __finished_object_storage(self, creation: _ObjectCreation):
        logging.debug(
            f"del object cache "
            f"object_name={creation.object_name!r}, "
            f"object_id={creation.object_id!r}, "
        )
        self._queue_deleted_object_ids.put_nowait(creation.object_id)
