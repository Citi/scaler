import socket
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import logging_test_name


class TestCluster(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_manual_storage_address(self):
        N_TASKS = 8
        N_WORKERS = N_TASKS

        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        object_storage_address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        combo = SchedulerClusterCombo(
            address=address, object_storage_address=object_storage_address, n_workers=N_WORKERS
        )

        with Client(address=address) as client:
            future = client.submit(round, 3.14)
            self.assertEqual(future.result(), round(3.14))

        combo.shutdown()

    def test_scheduler_listens_once_constructed(self):
        """The combo returns only after its scheduler accepts connections, so a client with a short timeout fits."""
        port = get_available_tcp_port()
        combo = SchedulerClusterCombo(address=f"tcp://127.0.0.1:{port}", n_workers=1)
        try:
            socket.create_connection(("127.0.0.1", port)).close()
        finally:
            combo.shutdown()
