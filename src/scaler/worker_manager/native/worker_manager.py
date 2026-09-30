from __future__ import annotations

import logging
import multiprocessing.connection
import signal
import uuid
from typing import List, Set

from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.utility.exitcode import describe_exitcode
from scaler.worker.worker import Worker
from scaler.worker_manager.local_process import (
    LOCAL_PROCESS_POLL_INTERVAL_SECONDS,
    poll_local_processes,
    stop_local_process,
)
from scaler.worker_manager.mixins import UnitHandle, UnitProvisioner
from scaler.worker_manager.runner import WorkerManagerRunner

logger = logging.getLogger(__name__)


class NativeWorkerProvisioner(UnitProvisioner):
    def __init__(self, config: NativeWorkerManagerConfig) -> None:
        self._worker_scheduler_address = config.worker_manager_config.effective_worker_scheduler_address
        self._object_storage_address = config.worker_manager_config.object_storage_address
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._worker_manager_id = config.worker_manager_config.worker_manager_id.encode()
        self._io_threads = config.worker_config.io_threads
        self._task_queue_size = config.worker_config.per_worker_task_queue_size
        self._max_task_concurrency = config.worker_manager_config.max_task_concurrency
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._task_timeout_seconds = config.worker_config.task_timeout_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._garbage_collect_interval_seconds = config.worker_config.garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = config.worker_config.trim_memory_threshold_bytes
        self._hard_processor_suspend = config.worker_config.hard_processor_suspend
        self._event_loop = config.worker_config.event_loop
        self._preload = config.worker_config.preload
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._security_config = config.security

        if config.worker_type is not None:
            self._worker_prefix = config.worker_type
        elif config.mode == NativeWorkerManagerMode.FIXED:
            self._worker_prefix = "FIX"
        elif config.mode == NativeWorkerManagerMode.DYNAMIC:
            self._worker_prefix = "NAT"
        else:
            raise ValueError(f"worker_type is not set and mode is unrecognised: {config.mode!r}")

    def _create_worker(self, unit_id: str) -> Worker:
        return Worker(
            name=f"{self._worker_prefix}|{unit_id}",
            address=self._worker_scheduler_address,
            object_storage_address=self._object_storage_address,
            preload=self._preload,
            capabilities=self._capabilities,
            io_threads=self._io_threads,
            task_queue_size=self._task_queue_size,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            task_timeout_seconds=self._task_timeout_seconds,
            death_timeout_seconds=self._death_timeout_seconds,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            hard_processor_suspend=self._hard_processor_suspend,
            event_loop=self._event_loop,
            logging_paths=self._logging_paths,
            logging_level=self._logging_level,
            worker_manager_id=self._worker_manager_id,
            security_config=self._security_config,
        )

    def run_fixed(self) -> None:
        workers: List[Worker] = []
        for _ in range(self._max_task_concurrency):
            worker = self._create_worker(uuid.uuid4().hex)
            worker.start()
            workers.append(worker)

        terminated_by_us: set[Worker] = set()

        def _on_signal(sig: int, frame: object) -> None:
            logger.info("NativeWorkerProvisioner (FIXED): received signal %d, terminating workers", sig)
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    terminated_by_us.add(worker)

        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)

        workers_by_sentinel = {worker.sentinel: worker for worker in workers}
        while workers_by_sentinel:
            for sentinel in multiprocessing.connection.wait(list(workers_by_sentinel)):
                worker = workers_by_sentinel.pop(sentinel)
                worker.join()

                if worker in terminated_by_us:
                    logger.info(
                        f"native worker {worker.identity!r} stopped (exitcode={describe_exitcode(worker.exitcode)})"
                    )
                elif worker.exitcode == 0:
                    # A worker exits 0 only when it was told to stop (by the scheduler or a
                    # cancellation), never as a symptom of a problem, even though this manager
                    # was not the one that asked.
                    logger.info(f"native worker {worker.identity!r} shut down cleanly")
                else:
                    logger.warning(
                        f"native worker {worker.identity!r} exited unexpectedly "
                        f"(exitcode={describe_exitcode(worker.exitcode)})"
                    )

    async def create_unit(self, unit_id: str) -> UnitHandle:
        worker = self._create_worker(unit_id)
        worker.start()
        logger.info(f"started native worker {worker.identity!r}")
        return worker

    async def destroy_unit(self, handle: UnitHandle) -> None:
        assert isinstance(handle, Worker)
        await stop_local_process(handle)
        logger.info(f"stopped native worker {handle.identity!r}")

    async def poll_units(self, handles: Set[UnitHandle]) -> Set[UnitHandle]:
        return set(poll_local_processes({handle for handle in handles if isinstance(handle, Worker)}))

    def max_units(self) -> int:
        return self._max_task_concurrency

    def task_concurrency_per_unit(self) -> int:
        return 1

    def poll_interval_seconds(self) -> int:
        return LOCAL_PROCESS_POLL_INTERVAL_SECONDS


class NativeWorkerManager:
    def __init__(self, config: NativeWorkerManagerConfig) -> None:
        self._config = config

    @property
    def config(self) -> NativeWorkerManagerConfig:
        return self._config

    def run(self) -> None:
        provisioner = NativeWorkerProvisioner(self._config)

        if self._config.mode == NativeWorkerManagerMode.FIXED:
            provisioner.run_fixed()
            return

        runner = WorkerManagerRunner(
            name="worker_manager_native",
            worker_manager_config=self._config.worker_manager_config,
            heartbeat_interval_seconds=self._config.worker_config.heartbeat_interval_seconds,
            capabilities=self._config.worker_config.per_worker_capabilities.capabilities,
            provisioner=provisioner,
            io_threads=self._config.worker_config.io_threads,
            security_config=self._config.security,
        )
        runner.run()
