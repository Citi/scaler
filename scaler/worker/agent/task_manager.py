from typing import Dict, Optional, Set, Tuple

from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.common import TaskStatus
from scaler.protocol.python.message import Task, TaskCancel, TaskResult
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.mixins import Looper
from scaler.utility.queues.async_sorted_priority_queue import AsyncSortedPriorityQueue
from scaler.worker.agent.mixins import ProcessorManager, TaskManager

_SUSPENDED_TASKS_PRIORITY = 1
_QUEUED_TASKS_PRIORITY = 2


class VanillaTaskManager(Looper, TaskManager):
    def __init__(self, task_timeout_seconds: int):
        self._task_timeout_seconds = task_timeout_seconds

        self._queued_task_id_to_task: Dict[bytes, Task] = dict()

        # Queued tasks are sorted first by task's priorities, then suspended tasks are prioritized over non yet started
        # tasks.
        self._queued_task_ids = AsyncSortedPriorityQueue()

        self._processing_task_ids: Set[bytes] = set()  # Tasks associated with a processor, including suspended tasks

        self._connector_external: Optional[AsyncConnector] = None
        self._processor_manager: Optional[ProcessorManager] = None

    def register(self, connector: AsyncConnector, processor_manager: ProcessorManager):
        self._connector_external = connector
        self._processor_manager = processor_manager

    async def on_task_new(self, task: Task):
        task_priority = self.__get_task_priority(task)

        self._queued_task_id_to_task[task.task_id] = task
        self._queued_task_ids.put_nowait(((task_priority, _QUEUED_TASKS_PRIORITY), task.task_id))

        await self.__suspend_if_priority_is_lower(task_priority)

    async def on_cancel_task(self, task_cancel: TaskCancel):
        task_id = task_cancel.task_id

        task_not_found = task_id not in self._processing_task_ids and task_id not in self._queued_task_id_to_task
        task_is_processing = task_id in self._processing_task_ids

        if task_not_found or (task_is_processing and not task_cancel.flags.force):
            result = TaskResult.new_msg(task_id, TaskStatus.NotFound)
            await self._connector_external.send(result)
            return

        # A suspended task will be both processing AND queued

        if task_cancel.task_id in self._queued_task_id_to_task:
            canceled_task = self._queued_task_id_to_task.pop(task_cancel.task_id)
            self._queued_task_ids.remove(task_cancel.task_id)

        if task_is_processing:
            self._processing_task_ids.remove(task_cancel.task_id)
            canceled_task = await self._processor_manager.on_cancel_task(task_cancel.task_id)

        assert canceled_task is not None

        payload = [canceled_task.get_message().to_bytes()] if task_cancel.flags.retrieve_task_object else []
        result = TaskResult.new_msg(task_id=task_id, status=TaskStatus.Canceled, metadata=b"", results=payload)
        await self._connector_external.send(result)

    async def on_task_result(self, result: TaskResult):
        if result.task_id in self._queued_task_id_to_task:
            # Finishing a queued task might happen if a task ended during the suspension process.
            self._queued_task_id_to_task.pop(result.task_id)
            self._queued_task_ids.remove(result.task_id)

        self._processing_task_ids.remove(result.task_id)

        await self._connector_external.send(result)

    async def routine(self):
        await self.__processing_task()

    def get_queued_size(self):
        return self._queued_task_ids.qsize()

    async def __processing_task(self):
        await self._processor_manager.wait_until_can_accept_task()

        _, task_id = await self._queued_task_ids.get()
        task = self._queued_task_id_to_task.pop(task_id)

        if task_id not in self._processing_task_ids:
            self._processing_task_ids.add(task_id)
            await self._processor_manager.on_task(task)
        else:
            self._processor_manager.on_resume_task(task_id)

    async def __suspend_if_priority_is_lower(self, new_task_priority: int):
        current_task = self._processor_manager.current_task()

        if current_task is None:
            return

        current_task_priority = self.__get_task_priority(current_task)

        if new_task_priority >= current_task_priority:
            return

        self._queued_task_ids.put_nowait(((current_task_priority, _SUSPENDED_TASKS_PRIORITY), current_task.task_id))
        self._queued_task_id_to_task[current_task.task_id] = current_task

        await self._processor_manager.on_suspend_task(current_task.task_id)

    @staticmethod
    def __get_task_priority(task: Task) -> int:
        return retrieve_task_flags_from_task(task).priority

    @staticmethod
    def __is_suspended_task(priority: Tuple[int, int]) -> bool:
        return priority[1] == _SUSPENDED_TASKS_PRIORITY
