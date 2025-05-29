import abc
from typing import Optional, Set

from scaler.protocol.python.common import ObjectMetadata
from scaler.protocol.python.message import (
    ClientDisconnect,
    ClientHeartbeat,
    DisconnectRequest,
    GraphTask,
    GraphTaskCancel,
    ObjectInstruction,
    Task,
    TaskCancel,
    TaskResult,
    WorkerHeartbeat,
)
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.mixins import Reporter


class ObjectManager(Reporter):
    @abc.abstractmethod
    async def on_object_instruction(self, source: bytes, request: ObjectInstruction):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_add_object(
        self,
        object_user: ClientID,
        object_id: ObjectID,
        object_type: ObjectMetadata.ObjectContentType,
        object_name: bytes,
    ):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_del_objects(self, object_user: ClientID, object_ids: Set[ObjectID]):
        raise NotImplementedError()

    @abc.abstractmethod
    def clean_client(self, client: ClientID):
        raise NotImplementedError()

    @abc.abstractmethod
    def has_object(self, object_id: ObjectID) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_object_name(self, object_id: ObjectID) -> bytes:
        raise NotImplementedError()


class ClientManager(Reporter):
    @abc.abstractmethod
    def get_client_task_ids(self, client: ClientID) -> Set[TaskID]:
        raise NotImplementedError()

    @abc.abstractmethod
    def has_client_id(self, client_id: ClientID) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_client_id(self, task_id: TaskID) -> Optional[ClientID]:
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_begin(self, client: ClientID, task_id: TaskID):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_finish(self, task_id: TaskID) -> bytes:
        raise NotImplementedError()

    async def on_heartbeat(self, client: ClientID, info: ClientHeartbeat):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_client_disconnect(self, client: ClientID, request: ClientDisconnect):
        raise NotImplementedError()


class GraphTaskManager(Reporter):
    @abc.abstractmethod
    async def on_graph_task(self, client: ClientID, graph_task: GraphTask):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_graph_task_cancel(self, client: ClientID, graph_task_cancel: GraphTaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_graph_sub_task_done(self, result: TaskResult) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def is_graph_sub_task(self, task_id: TaskID) -> bool:
        raise NotImplementedError()


class TaskManager(Reporter):
    @abc.abstractmethod
    async def on_task_new(self, client: ClientID, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, client: ClientID, task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_done(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_reroute(self, task_id: TaskID):
        raise NotImplementedError()


class WorkerManager(Reporter):
    @abc.abstractmethod
    async def assign_task_to_worker(self, task: Task) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_cancel(self, task_cancel: TaskCancel):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_result(self, task_result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat(self, worker: WorkerID, info: WorkerHeartbeat):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_client_shutdown(self, client: ClientID):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_disconnect(self, source: WorkerID, request: DisconnectRequest):
        raise NotImplementedError()

    @abc.abstractmethod
    def has_available_worker(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_worker_ids(self) -> Set[WorkerID]:
        raise NotImplementedError()
