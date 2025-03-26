
import functools
import os
import random
import time
import unittest
from concurrent.futures import CancelledError

from tests.utility import get_available_tcp_port, logging_test_name

from scaler import Client, SchedulerClusterCombo
from scaler.utility.exceptions import MissingObjects, ProcessorDiedError
from scaler.utility.logging.scoped_logger import ScopedLogger
from scaler.utility.logging.utility import setup_logger

import runpy
import os

def noop(sec: int):
    return sec * 1


def noop_sleep(sec: int):
    time.sleep(sec)
    return sec


def heavy_function(sec: int, payload: bytes):
    return len(payload) * sec


def raise_exception(foo: int):
    if foo == 11:
        raise ValueError("foo cannot be 100")


class TestExamples(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.address = f"tcp://127.0.0.1:2345"
        self._workers = 3
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=self._workers, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()
        pass

    def test_examples(self):
        assert(os.system("PYTHONPATH=. python examples/disconnect_client.py") == 0)
        assert(os.system("PYTHONPATH=. python examples/graphtask_nested_client.py") == 0)
        assert(os.system("PYTHONPATH=. python examples/nested_client.py") == 0)
        assert(os.system("PYTHONPATH=. python examples/simple_client.py") == 0)
        assert(os.system("PYTHONPATH=. python examples/graphtask_client.py") == 0)
        assert(os.system("PYTHONPATH=. python examples/map_client.py") == 0)
