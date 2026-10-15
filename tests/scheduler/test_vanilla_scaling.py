import time
import unittest
from typing import Dict, Optional

from scaler.protocol.capnp import Resource, Task, WorkerHeartbeat, WorkerManagerHeartbeat
from scaler.scheduler.policies.scaling.types import WorkerManagerBounds, WorkerManagerSnapshot
from scaler.scheduler.policies.scaling.vanilla import VanillaScalingPolicy
from scaler.scheduler.policies.utility import create_scaling_policy
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.snapshot import InformationSnapshot


class TestVanillaScalingPolicy(unittest.TestCase):
    """Unit tests for VanillaScalingPolicy declarative emission."""

    def setUp(self):
        setup_logger()
        self.policy = VanillaScalingPolicy({})

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


class TestVanillaBounds(unittest.TestCase):
    """Vanilla keeps each manager within its bounds, and counts workers held at other managers' floors as supply."""

    def setUp(self):
        setup_logger()
        self.policy = VanillaScalingPolicy(
            {b"native": WorkerManagerBounds(max_task_concurrency=8, min_task_concurrency=8)}
        )

    def _desired(
        self,
        policy: VanillaScalingPolicy,
        manager_id: bytes,
        task_count: int,
        worker_counts: Dict[bytes, int],
        max_task_concurrency: int = 100,
    ) -> int:
        tasks = {TaskID.generate_task_id(): _create_mock_task(TaskID.generate_task_id(), {}) for _ in range(task_count)}
        workers = {
            WorkerID(f"{other_id.decode()}-{worker_i}".encode()): _create_mock_worker_heartbeat({})
            for other_id, count in worker_counts.items()
            for worker_i in range(count)
        }
        managed = [worker_id for worker_id in workers if worker_id.startswith(manager_id + b"-")]
        snapshots = {
            other_id: WorkerManagerSnapshot(
                worker_manager_id=other_id, max_task_concurrency=100, worker_count=count, last_seen_at=time.time()
            )
            for other_id, count in worker_counts.items()
        }
        heartbeat = _create_worker_manager_heartbeat(manager_id, max_task_concurrency=max_task_concurrency)
        commands = policy.get_scaling_commands(
            InformationSnapshot(tasks=tasks, workers=workers), heartbeat, managed, snapshots
        )
        return commands[0].setDesiredTaskConcurrencyRequests[0].taskConcurrency

    def test_a_pinned_manager_holds_its_count_with_no_tasks(self):
        """A manager whose floor equals its cap keeps that count when idle."""
        self.assertEqual(self._desired(self.policy, b"native", 0, {b"native": 8}), 8)

    def test_an_idle_pinned_pool_keeps_the_scaling_manager_at_zero(self):
        """Few tasks over a pinned pool ask the scaling manager for nothing, not ceil(tasks / 10)."""
        self.assertEqual(self._desired(self.policy, b"cloud", 5, {b"native": 8, b"cloud": 0}), 0)

    def test_load_beyond_the_pinned_pool_scales_the_other_manager_up(self):
        """Above the scale-up ratio the scaling manager still grows."""
        self.assertEqual(self._desired(self.policy, b"cloud", 100, {b"native": 8, b"cloud": 0}), 1)

    def test_a_pool_that_is_not_up_supplies_nothing(self):
        """Only connected workers at a floor count, so a pool that is still starting does not starve the tasks."""
        self.assertEqual(self._desired(self.policy, b"cloud", 5, {b"native": 0, b"cloud": 10}), 1)

    def test_workers_above_a_floor_are_not_subtracted(self):
        """Only the workers a floor holds count as supply; the rest can shrink."""
        policy = VanillaScalingPolicy({b"a": WorkerManagerBounds(max_task_concurrency=10, min_task_concurrency=2)})
        # ratio 40 / 46 < 1: needed ceil(40 / 10) = 4, minus the 2 workers a's floor holds.
        self.assertEqual(self._desired(policy, b"b", 40, {b"a": 6, b"b": 40}), 2)

    def test_the_cap_of_the_bounds_clamps_below_the_advertised_maximum(self):
        """A cap in the bounds wins when it is lower than the manager's own maximum."""
        policy = VanillaScalingPolicy({b"cloud": WorkerManagerBounds(max_task_concurrency=3)})
        self.assertEqual(self._desired(policy, b"cloud", 500, {b"cloud": 3}), 3)

    def test_the_floor_is_clamped_to_the_advertised_maximum(self):
        """A floor above what the manager advertises asks for that maximum."""
        self.assertEqual(self._desired(self.policy, b"native", 0, {b"native": 0}, max_task_concurrency=4), 4)

    def test_bounds_parse_from_the_scaling_string(self):
        """`vanilla[id:max[:min], ...]` sets each manager's bounds; an empty max leaves the cap to the manager."""
        policy = create_scaling_policy("vanilla[native-local:8:8, ecs-burst:100, warm::2]")
        assert isinstance(policy, VanillaScalingPolicy)
        self.assertEqual(
            policy._bounds,
            {
                b"native-local": WorkerManagerBounds(8, 8),
                b"ecs-burst": WorkerManagerBounds(100, 0),
                b"warm": WorkerManagerBounds(None, 2),
            },
        )

    def test_invalid_bounds_are_refused(self):
        for scaling in [
            "vanilla[]",
            "vanilla[native]",
            "vanilla[native:8:8",
            "vanilla[:8]",
            "vanilla[native:4:8]",
            "vanilla[native:-1]",
            "vanilla[native:many]",
            "vanilla[native:8:8:8]",
            "vanilla[native:8, native:4]",
            "capability[native:8]",
        ]:
            with self.subTest(scaling=scaling):
                with self.assertRaises(ValueError):
                    create_scaling_policy(scaling)


class TestVanillaDeclarativeEquivalents(unittest.TestCase):
    """Declarative equivalents of the master-branch vanilla shutdown tests."""

    def setUp(self):
        setup_logger()
        self.policy = VanillaScalingPolicy({})

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
