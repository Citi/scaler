
import unittest

from typing import Dict, Set

from scaler.utility.logging.utility import setup_logger
from scaler.scheduler.allocators.tagged_allocator import TaggedAllocator
from scaler.protocol.python.message import Task

from tests.utility import logging_test_name

MAX_TASKS_PER_WORKER = 10


class TestTaggedAllocator(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_assign_task(self):
        allocator = TaggedAllocator(max_tasks_per_worker=MAX_TASKS_PER_WORKER)

        regular_task = self.__create_task(b"task_regular", set())

        # No worker, should return None
        assigned_worker = allocator.assign_task(regular_task)
        self.assertIsNone(assigned_worker)

        # Adds a bunch of workers
        worker_added = allocator.add_worker(b"worker_regular", set())
        self.assertTrue(worker_added)
        worker_added = allocator.add_worker(b"worker_gpu", {"gpu"})
        self.assertTrue(worker_added)

        self.assertEqual(allocator.get_worker_ids(), {b"worker_regular", b"worker_gpu"})

        # Assign a task to the GPU worker
        gpu_task = self.__create_task(b"task_gpu", {"gpu"})
        assigned_worker = allocator.assign_task(gpu_task)
        self.assertEqual(assigned_worker, b"worker_gpu")

        # Assign a task with a non-supported tag should fail
        mac_os_task = self.__create_task(b"task_mac_os", {"mac_os"})
        assigned_worker = allocator.assign_task(mac_os_task)
        self.assertIsNone(assigned_worker)

        # Assign a task without tag
        assigned_worker = allocator.assign_task(regular_task)
        self.assertEqual(assigned_worker, b"worker_regular")

        # Assign should fail when the number of tasks exceeds MAX_TASKS_PER_WORKER

        for i in range(0, (MAX_TASKS_PER_WORKER * 2) - 2):
            self.assertTrue(allocator.has_available_worker())

            task = self.__create_task(f"task_{i}".encode(), set())
            assigned_worker = allocator.assign_task(task)
            self.assertIsNotNone(assigned_worker)

        self.assertFalse(allocator.has_available_worker())

        overloaded_task = self.__create_task(b"task_overload", set())
        assigned_worker = allocator.assign_task(overloaded_task)
        self.assertIsNone(assigned_worker)

    def test_remove_worker(self):
        N_TASKS = MAX_TASKS_PER_WORKER + 3

        allocator = TaggedAllocator(max_tasks_per_worker=MAX_TASKS_PER_WORKER)

        allocator.add_worker(b"worker_1", set())
        allocator.add_worker(b"worker_2", set())

        # Adds a bunch of tasks

        worker_id_to_tasks: Dict[bytes, Set[bytes]] = {
            b"worker_1": set(),
            b"worker_2": set(),
        }

        for i in range(0, N_TASKS):
            task = self.__create_task(f"task_{i}".encode(), set())
            assigned_worker = allocator.assign_task(task)
            worker_id_to_tasks[assigned_worker].add(task.task_id)

        # Tasks should be balanced between the two workers

        for workers_tasks in worker_id_to_tasks.values():
            self.assertAlmostEqual(len(workers_tasks), N_TASKS / 2, delta=1.0)

        # Removes the two workers

        worker_tasks = allocator.remove_worker(b"worker_1")
        self.assertSetEqual(set(worker_tasks), worker_id_to_tasks[b"worker_1"])

        worker_tasks = allocator.remove_worker(b"worker_2")
        self.assertSetEqual(set(worker_tasks), worker_id_to_tasks[b"worker_2"])

    def test_balancing(self):
        N_TASKS = MAX_TASKS_PER_WORKER // 2 * 2  # must be even

        allocator = TaggedAllocator(max_tasks_per_worker=MAX_TASKS_PER_WORKER)

        allocator.add_worker(b"worker_1", {"linux", "gpu"})

        # Assign a few tasks

        for i in range(0, N_TASKS // 2):
            allocator.assign_task(self.__create_task(f"gpu_task_{i}".encode(), {"gpu"}))
            allocator.assign_task(self.__create_task(f"linux+gpu_task_{i}".encode(), {"linux", "gpu"}))

        self.assertDictEqual(allocator.balance(), {})

        # Adds a worker that cannot accept the balanced tasks

        allocator.add_worker(b"worker_2", {"windows"})
        self.assertDictEqual(allocator.balance(), {})

        # Adds a worker that can accept some of the tasks

        allocator.add_worker(b"worker_3", {"gpu"})
        balancing_advice = allocator.balance()

        avg_tasks_per_worker = N_TASKS / 3

        self.assertListEqual(list(balancing_advice.keys()), [b"worker_1"])

        self.assertAlmostEqual(len(balancing_advice[b"worker_1"]), avg_tasks_per_worker, delta=1.0)
        self.assertSetEqual(
            set(balancing_advice[b"worker_1"]),
            {f"gpu_task_{(N_TASKS // 2) - i - 1}".encode() for i in range(0, int(avg_tasks_per_worker))},
            msg="youngest task should be advised first for balancing.",
        )

        # Adds a fourth worker that can accept all tasks

        allocator.add_worker(b"worker_4", {"gpu", "linux"})
        balancing_advice = allocator.balance()

        avg_tasks_per_worker = N_TASKS / 4

        self.assertListEqual(list(balancing_advice.keys()), [b"worker_1"])
        self.assertAlmostEqual(len(balancing_advice[b"worker_1"]), avg_tasks_per_worker * 2, delta=1.0)

    @staticmethod
    def __create_task(task_id: bytes, tags: Set[str]) -> Task:
        return Task.new_msg(task_id, b"client_id", tags, b"", b"function_id", [])
