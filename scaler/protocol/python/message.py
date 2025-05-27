import abc
import dataclasses
import enum
import os
from typing import List, Optional, Set, Type

import bidict

from scaler.protocol.capnp._python import _message  # noqa
from scaler.protocol.python.common import ObjectMetadata, ObjectStorageAddress, TaskStatus
from scaler.protocol.python.mixins import Message
from scaler.protocol.python.status import (
    BinderStatus,
    ClientManagerStatus,
    ObjectManagerStatus,
    ProcessorStatus,
    Resource,
    TaskManagerStatus,
    WorkerManagerStatus,
)
from scaler.utility.object_id import ObjectID


class Task(Message):
    class Argument(metaclass=abc.ABCMeta):
        @abc.abstractmethod
        def to_capnp(self) -> _message.Task.Argument:
            raise NotImplementedError()

    @dataclasses.dataclass(frozen=True)
    class TaskArgument(Argument):
        task_id: bytes

        def to_capnp(self) -> _message.Task.Argument:
            return _message.Task.Argument(type=_message.Task.Argument.ArgumentType.task, data=self.task_id)

        def __repr__(self) -> str:
            return f"TaskArgument({self.task_id.hex()})"

    @dataclasses.dataclass(frozen=True)
    class ObjectArgument(Argument):
        object_id: ObjectID

        def to_capnp(self) -> _message.Task.Argument:
            return _message.Task.Argument(
                type=_message.Task.Argument.ArgumentType.objectID,
                data=self.object_id.bytes()
            )

        def __repr__(self) -> str:
            return f"ObjectArgument({repr(self.object_id)})"

    def __init__(self, msg):
        super().__init__(msg)

    def __repr__(self):
        return (
            f"Task(task_id={self.task_id.hex()}, source={self.source.hex()}, metadata={self.metadata.hex()},"
            f"func_object_id={repr(self.func_object_id)}, function_args={self.function_args})"
        )

    @property
    def task_id(self) -> bytes:
        return self._msg.taskId

    @property
    def source(self) -> bytes:
        return self._msg.source

    @property
    def metadata(self) -> bytes:
        return self._msg.metadata

    @property
    def func_object_id(self) -> Optional[ObjectID]:
        if len(self._msg.funcObjectId) > 0:
            return ObjectID(self._msg.funcObjectId)
        else:
            return None

    @property
    def function_args(self) -> List[Argument]:
        return [self._from_capnp_task_argument(arg) for arg in self._msg.functionArgs]

    @staticmethod
    def new_msg(
        task_id: bytes,
        source: bytes,
        metadata: bytes,
        func_object_id: Optional[ObjectID],
        function_args: List[Argument],
    ) -> "Task":
        return Task(
            _message.Task(
                taskId=task_id,
                source=source,
                metadata=metadata,
                funcObjectId=func_object_id.bytes() if func_object_id is not None else b"",
                functionArgs=[arg.to_capnp() for arg in function_args],
            )
        )

    @staticmethod
    def _from_capnp_task_argument(value: _message.Task.Argument) -> Argument:
        if value.type.raw ==  _message.Task.Argument.ArgumentType.task:
            return Task.TaskArgument(value.data)
        else:
            return Task.ObjectArgument(ObjectID(value.data))


class TaskCancel(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @dataclasses.dataclass
    class TaskCancelFlags:
        force: bool
        retrieve_task_object: bool

    @property
    def task_id(self) -> bytes:
        return self._msg.taskId

    @property
    def flags(self) -> TaskCancelFlags:
        return TaskCancel.TaskCancelFlags(
            force=self._msg.flags.force, retrieve_task_object=self._msg.flags.retrieveTaskObject
        )

    @staticmethod
    def new_msg(task_id: bytes, flags: Optional[TaskCancelFlags] = None) -> "TaskCancel":
        if flags is None:
            flags = TaskCancel.TaskCancelFlags(force=False, retrieve_task_object=False)

        return TaskCancel(
            _message.TaskCancel(
                taskId=task_id,
                flags=_message.TaskCancel.TaskCancelFlags(
                    force=flags.force, retrieveTaskObject=flags.retrieve_task_object
                ),
            )
        )


class TaskResult(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def task_id(self) -> bytes:
        return self._msg.taskId

    @property
    def status(self) -> TaskStatus:
        return TaskStatus(self._msg.status.raw)

    @property
    def metadata(self) -> bytes:
        return self._msg.metadata

    @property
    def results(self) -> List[bytes]:
        return self._msg.results

    @staticmethod
    def new_msg(
        task_id: bytes, status: TaskStatus, metadata: Optional[bytes] = None, results: Optional[List[bytes]] = None
    ) -> "TaskResult":
        if metadata is None:
            metadata = bytes()

        if results is None:
            results = list()

        return TaskResult(_message.TaskResult(taskId=task_id, status=status.value, metadata=metadata, results=results))


class GraphTask(Message):
    def __init__(self, msg):
        super().__init__(msg)

    def __repr__(self):
        return (
            f"GraphTask({os.linesep}"
            f"    task_id={self.task_id.hex()},{os.linesep}"
            f"    targets=[{os.linesep}"
            f"        {[target.hex() + ',' + os.linesep for target in self.targets]}"
            f"    ]\n"
            f"    graph={self.graph}\n"
            f")"
        )

    @property
    def task_id(self) -> bytes:
        return self._msg.taskId

    @property
    def source(self) -> bytes:
        return self._msg.source

    @property
    def targets(self) -> List[bytes]:
        return self._msg.targets

    @property
    def graph(self) -> List[Task]:
        return [Task(task) for task in self._msg.graph]

    @staticmethod
    def new_msg(task_id: bytes, source: bytes, targets: List[bytes], graph: List[Task]) -> "GraphTask":
        return GraphTask(
            _message.GraphTask(
                taskId=task_id, source=source, targets=targets, graph=[task.get_message() for task in graph]
            )
        )


class GraphTaskCancel(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def task_id(self) -> bytes:
        return self._msg.taskId

    @staticmethod
    def new_msg(task_id: bytes) -> "GraphTaskCancel":
        return GraphTaskCancel(_message.GraphTaskCancel(taskId=task_id))

    def get_message(self):
        return self._msg


class ClientHeartbeat(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def resource(self) -> Resource:
        return Resource(self._msg.resource)

    @property
    def latency_us(self) -> int:
        return self._msg.latencyUS

    @staticmethod
    def new_msg(resource: Resource, latency_us: int) -> "ClientHeartbeat":
        return ClientHeartbeat(_message.ClientHeartbeat(resource=resource.get_message(), latencyUS=latency_us))


class ClientHeartbeatEcho(Message):
    def __init__(self, msg):
        super().__init__(msg)

    def object_storage_address(self) -> ObjectStorageAddress:
        return ObjectStorageAddress(self._msg.objectStorageAddress)

    @staticmethod
    def new_msg(object_storage_address: ObjectStorageAddress) -> "ClientHeartbeatEcho":
        return ClientHeartbeatEcho(
            _message.ClientHeartbeatEcho(objectStorageAddress=object_storage_address.get_message())
        )


class WorkerHeartbeat(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def agent(self) -> Resource:
        return Resource(self._msg.agent)

    @property
    def rss_free(self) -> int:
        return self._msg.rssFree

    @property
    def queued_tasks(self) -> int:
        return self._msg.queuedTasks

    @property
    def latency_us(self) -> int:
        return self._msg.latencyUS

    @property
    def task_lock(self) -> bool:
        return self._msg.taskLock

    @property
    def processors(self) -> List[ProcessorStatus]:
        return [ProcessorStatus(p) for p in self._msg.processors]

    @staticmethod
    def new_msg(
        agent: Resource,
        rss_free: int,
        queued_tasks: int,
        latency_us: int,
        task_lock: bool,
        processors: List[ProcessorStatus],
    ) -> "WorkerHeartbeat":
        return WorkerHeartbeat(
            _message.WorkerHeartbeat(
                agent=agent.get_message(),
                rssFree=rss_free,
                queuedTasks=queued_tasks,
                latencyUS=latency_us,
                taskLock=task_lock,
                processors=[p.get_message() for p in processors],
            )
        )


class WorkerHeartbeatEcho(Message):
    def __init__(self, msg):
        super().__init__(msg)

    def object_storage_address(self) -> ObjectStorageAddress:
        return ObjectStorageAddress(self._msg.objectStorageAddress)

    @staticmethod
    def new_msg(object_storage_address: ObjectStorageAddress) -> "WorkerHeartbeatEcho":
        return WorkerHeartbeatEcho(
            _message.WorkerHeartbeatEcho(objectStorageAddress=object_storage_address.get_message())
        )


class ObjectInstruction(Message):
    class ObjectInstructionType(enum.Enum):
        Create = _message.ObjectInstruction.ObjectInstructionType.create
        Delete = _message.ObjectInstruction.ObjectInstructionType.delete
        Clear = _message.ObjectInstruction.ObjectInstructionType.clear

    def __init__(self, msg):
        super().__init__(msg)

    @property
    def instruction_type(self) -> ObjectInstructionType:
        return ObjectInstruction.ObjectInstructionType(self._msg.instructionType.raw)

    @property
    def object_user(self) -> bytes:
        return self._msg.objectUser

    @property
    def object_metadata(self) -> ObjectMetadata:
        return ObjectMetadata(self._msg.objectMetadata)

    @staticmethod
    def new_msg(
        instruction_type: ObjectInstructionType, object_user: bytes, object_metadata: ObjectMetadata
    ) -> "ObjectInstruction":
        return ObjectInstruction(
            _message.ObjectInstruction(
                instructionType=instruction_type.value,
                objectUser=object_user,
                objectMetadata=object_metadata.get_message(),
            )
        )


class DisconnectRequest(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def worker(self) -> bytes:
        return self._msg.worker

    @staticmethod
    def new_msg(worker: bytes) -> "DisconnectRequest":
        return DisconnectRequest(_message.DisconnectRequest(worker=worker))


@dataclasses.dataclass
class DisconnectResponse(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def worker(self) -> bytes:
        return self._msg.worker

    @staticmethod
    def new_msg(worker: bytes) -> "DisconnectResponse":
        return DisconnectResponse(_message.DisconnectResponse(worker=worker))


class ClientDisconnect(Message):
    class DisconnectType(enum.Enum):
        Disconnect = _message.ClientDisconnect.DisconnectType.disconnect
        Shutdown = _message.ClientDisconnect.DisconnectType.shutdown

    def __init__(self, msg):
        super().__init__(msg)

    @property
    def disconnect_type(self) -> DisconnectType:
        return ClientDisconnect.DisconnectType(self._msg.disconnectType.raw)

    @staticmethod
    def new_msg(disconnect_type: DisconnectType) -> "ClientDisconnect":
        return ClientDisconnect(_message.ClientDisconnect(disconnectType=disconnect_type.value))


class ClientShutdownResponse(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def accepted(self) -> bool:
        return self._msg.accepted

    @staticmethod
    def new_msg(accepted: bool) -> "ClientShutdownResponse":
        return ClientShutdownResponse(_message.ClientShutdownResponse(accepted=accepted))


class StateClient(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @staticmethod
    def new_msg() -> "StateClient":
        return StateClient(_message.StateClient())


class StateObject(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @staticmethod
    def new_msg() -> "StateObject":
        return StateObject(_message.StateObject())


class StateBalanceAdvice(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def worker_id(self) -> bytes:
        return self._msg.workerId

    @property
    def task_ids(self) -> List[bytes]:
        return self._msg.taskIds

    @staticmethod
    def new_msg(worker_id: bytes, task_ids: List[bytes]) -> "StateBalanceAdvice":
        return StateBalanceAdvice(_message.StateBalanceAdvice(workerId=worker_id, taskIds=task_ids))


class StateScheduler(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def binder(self) -> BinderStatus:
        return BinderStatus(self._msg.binder)

    @property
    def scheduler(self) -> Resource:
        return Resource(self._msg.scheduler)

    @property
    def rss_free(self) -> int:
        return self._msg.rssFree

    @property
    def client_manager(self) -> ClientManagerStatus:
        return ClientManagerStatus(self._msg.clientManager)

    @property
    def object_manager(self) -> ObjectManagerStatus:
        return ObjectManagerStatus(self._msg.objectManager)

    @property
    def task_manager(self) -> TaskManagerStatus:
        return TaskManagerStatus(self._msg.taskManager)

    @property
    def worker_manager(self) -> WorkerManagerStatus:
        return WorkerManagerStatus(self._msg.workerManager)

    @staticmethod
    def new_msg(
        binder: BinderStatus,
        scheduler: Resource,
        rss_free: int,
        client_manager: ClientManagerStatus,
        object_manager: ObjectManagerStatus,
        task_manager: TaskManagerStatus,
        worker_manager: WorkerManagerStatus,
    ) -> "StateScheduler":
        return StateScheduler(
            _message.StateScheduler(
                binder=binder.get_message(),
                scheduler=scheduler.get_message(),
                rssFree=rss_free,
                clientManager=client_manager.get_message(),
                objectManager=object_manager.get_message(),
                taskManager=task_manager.get_message(),
                workerManager=worker_manager.get_message(),
            )
        )


class StateWorker(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def worker_id(self) -> bytes:
        return self._msg.workerId

    @property
    def message(self) -> bytes:
        return self._msg.message

    @staticmethod
    def new_msg(worker_id: bytes, message: bytes) -> "StateWorker":
        return StateWorker(_message.StateWorker(workerId=worker_id, message=message))


class StateTask(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def task_id(self) -> bytes:
        return self._msg.taskId

    @property
    def function_name(self) -> bytes:
        return self._msg.functionName

    @property
    def status(self) -> TaskStatus:
        return TaskStatus(self._msg.status.raw)

    @property
    def worker(self) -> bytes:
        return self._msg.worker

    @property
    def metadata(self) -> bytes:
        return self._msg.metadata

    @staticmethod
    def new_msg(
        task_id: bytes, function_name: bytes, status: TaskStatus, worker: bytes, metadata: bytes = b""
    ) -> "StateTask":
        return StateTask(
            _message.StateTask(
                taskId=task_id, functionName=function_name, status=status.value, worker=worker, metadata=metadata
            )
        )


class StateGraphTask(Message):
    class NodeTaskType(enum.Enum):
        Normal = _message.StateGraphTask.NodeTaskType.normal
        Target = _message.StateGraphTask.NodeTaskType.target

    def __init__(self, msg):
        super().__init__(msg)

    @property
    def graph_task_id(self) -> bytes:
        return self._msg.graphTaskId

    @property
    def task_id(self) -> bytes:
        return self._msg.taskId

    @property
    def node_task_type(self) -> NodeTaskType:
        return StateGraphTask.NodeTaskType(self._msg.nodeTaskType.raw)

    @property
    def parent_task_ids(self) -> Set[bytes]:
        return set(self._msg.parentTaskIds)

    @staticmethod
    def new_msg(
        graph_task_id: bytes, task_id: bytes, node_task_type: NodeTaskType, parent_task_ids: Set[bytes]
    ) -> "StateGraphTask":
        return StateGraphTask(
            _message.StateGraphTask(
                graphTaskId=graph_task_id,
                taskId=task_id,
                nodeTaskType=node_task_type.value,
                parentTaskIds=list(parent_task_ids),
            )
        )


class ProcessorInitialized(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @staticmethod
    def new_msg() -> "ProcessorInitialized":
        return ProcessorInitialized(_message.ProcessorInitialized())


PROTOCOL: bidict.bidict[str, Type[Message]] = bidict.bidict(
    {
        "task": Task,
        "taskCancel": TaskCancel,
        "taskResult": TaskResult,
        "graphTask": GraphTask,
        "graphTaskCancel": GraphTaskCancel,
        "objectInstruction": ObjectInstruction,
        "clientHeartbeat": ClientHeartbeat,
        "clientHeartbeatEcho": ClientHeartbeatEcho,
        "workerHeartbeat": WorkerHeartbeat,
        "workerHeartbeatEcho": WorkerHeartbeatEcho,
        "disconnectRequest": DisconnectRequest,
        "disconnectResponse": DisconnectResponse,
        "stateClient": StateClient,
        "stateObject": StateObject,
        "stateBalanceAdvice": StateBalanceAdvice,
        "stateScheduler": StateScheduler,
        "stateWorker": StateWorker,
        "stateTask": StateTask,
        "stateGraphTask": StateGraphTask,
        "clientDisconnect": ClientDisconnect,
        "clientShutdownResponse": ClientShutdownResponse,
        "processorInitialized": ProcessorInitialized,
    }
)
