import unittest
from typing import Dict, Optional

from scaler.protocol.capnp import Resource, Task, WorkerHeartbeat, WorkerManagerHeartbeat
from scaler.scheduler.policies.scaling.vanilla import VanillaScalingPolicy
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.snapshot import InformationSnapshot


class TestVanillaScalingPolicy(unittest.TestCase):
    """Unit tests for VanillaScalingPolicy declarative emission."""

    def setUp(self):
        setup_logger()
        self.policy = VanillaScalingPolicy()

    def _single_request(self, snapshot, heartbeat, managed):
        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {})
        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(len(requests), 1)
        return requests[0]

    def test_idle_emits_desired_zero(self):
        """Empty state: ratio computes desired=0 -> policy unconditionally emits setDesired(0)."""
        snapshot = InformationSnapshot(tasks={}, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        request = self._single_request(snapshot, heartbeat, [])

        self.assertEqual(request.taskConcurrency, 0)

    def test_tasks_with_no_workers_targets_one(self):
        """Tasks present but no workers: desired = 1 to bootstrap."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(5)}
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        request = self._single_request(snapshot, heartbeat, [])

        self.assertEqual(request.taskConcurrency, 1)

    def test_high_task_ratio_targets_current_plus_one(self):
        """Task/worker ratio above upper threshold scales by +1 toward equilibrium."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(20)}
        managed = [WorkerID(b"w0")]
        workers = {wid: _create_mock_worker_heartbeat({}, queued_tasks=5) for wid in managed}
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=10)

        request = self._single_request(snapshot, heartbeat, managed)

        self.assertEqual(request.taskConcurrency, 2)

    def test_no_tasks_with_workers_targets_zero(self):
        """No tasks but managers exist: desired drains to 0."""
        workers = {
            WorkerID(b"w0"): _create_mock_worker_heartbeat({}, queued_tasks=0),
            WorkerID(b"w1"): _create_mock_worker_heartbeat({}, queued_tasks=1),
            WorkerID(b"w2"): _create_mock_worker_heartbeat({}, queued_tasks=2),
        }
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks={}, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        request = self._single_request(snapshot, heartbeat, managed)

        self.assertEqual(request.taskConcurrency, 0)

    def test_few_tasks_with_many_workers_targets_min_keep(self):
        """Low task ratio shrinks toward ceil(tasks / upper_task_ratio) min_keep."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(5)}
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=i) for i in range(10)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        request = self._single_request(snapshot, heartbeat, managed)

        # ceil(5 / 10) = 1 minimum to keep
        self.assertEqual(request.taskConcurrency, 1)

    def test_max_concurrency_clamps_then_emits(self):
        """Ratio asks for current+1 but cap clamps to current -> emits setDesired(current)."""
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(50)}
        managed = [WorkerID(b"w0"), WorkerID(b"w1")]
        workers = {wid: _create_mock_worker_heartbeat({}, queued_tasks=20) for wid in managed}
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=2)

        request = self._single_request(snapshot, heartbeat, managed)

        # Ratio would say desired=3, cap clamps to 2; emits setDesired(2).
        self.assertEqual(request.taskConcurrency, 2)


class TestVanillaDeclarativeEquivalents(unittest.TestCase):
    """Declarative equivalents of the master-branch vanilla shutdown tests."""

    def setUp(self):
        setup_logger()
        self.policy = VanillaScalingPolicy()

    def test_drain_all_when_idle(self):
        """With workers connected and no tasks, the policy targets desired=0 and emits
        setDesired(0) so the manager can drain its workers."""
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=0) for i in range(4)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks={}, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {})

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 0)

    def test_shrink_to_ratio_floor(self):
        """With few tasks relative to workers (ratio below lower threshold), the policy
        targets ceil(tasks/upper_task_ratio) and emits setDesired with that smaller count."""
        # 5 tasks, 10 connected workers -> ratio 0.5 < 1; floor = max(1, ceil(5/10)) = 1.
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(5)}
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=i) for i in range(10)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {})

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 1)

    def test_no_action_when_ratio_is_in_band(self):
        """With the task/worker ratio inside [lower, upper], the policy targets the current
        worker count and emits setDesired(current) unconditionally."""
        # 15 tasks, 5 workers -> ratio 3, in [1, 10]; desired = current = 5.
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(15)}
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat({}, queued_tasks=i) for i in range(5)}
        managed = list(workers.keys())
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {})

        self.assertEqual(len(commands), 1)
        requests = list(commands[0].setDesiredTaskConcurrencyRequests)
        self.assertEqual(requests[0].taskConcurrency, 5)


def _create_mock_task(task_id: TaskID, capabilities: dict) -> Task:
    client_id = ClientID.generate_client_id()
    return Task(
        taskId=task_id,
        source=client_id,
        metadata=b"",
        funcObjectId=ObjectID.generate_object_id(client_id),
        functionArgs=[],
        capabilities=capabilities,
    )


def _create_mock_worker_heartbeat(capabilities: dict, queued_tasks: int = 0) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        agent=Resource(cpu=1, rss=1000000),
        rssFree=500000,
        queueSize=10,
        queuedTasks=queued_tasks,
        latencyMicroseconds=100,
        taskLock=False,
        processors=[],
        capabilities=capabilities,
        workerManagerID=b"test",
    )


def _create_worker_manager_heartbeat(
    worker_manager_id: bytes, max_task_concurrency: int = 10, capabilities: Optional[Dict[str, int]] = None
) -> WorkerManagerHeartbeat:
    return WorkerManagerHeartbeat(
        maxTaskConcurrency=max_task_concurrency, capabilities=capabilities or {}, workerManagerID=worker_manager_id
    )
