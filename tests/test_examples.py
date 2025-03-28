import os
import unittest

from scaler.utility.logging.utility import setup_logger
from tests.utility import logging_test_name


class TestExamples(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def tearDown(self) -> None:
        pass

    def test_examples(self):
        assert os.system("PYTHONPATH=. python examples/disconnect_client.py") == 0
        assert os.system("PYTHONPATH=. python examples/graphtask_nested_client.py") == 0
        assert os.system("PYTHONPATH=. python examples/nested_client.py") == 0
        assert os.system("PYTHONPATH=. python examples/simple_client.py") == 0
        assert os.system("PYTHONPATH=. python examples/graphtask_client.py") == 0
        assert os.system("PYTHONPATH=. python examples/map_client.py") == 0
        assert os.system("PYTHONPATH=. python examples/send_object_client.py") == 0
        assert os.system("PYTHONPATH=. python examples/simple_scheduler.py") == 0
        assert os.system("PYTHONPATH=. python examples/simple_cluster.py") == 0
