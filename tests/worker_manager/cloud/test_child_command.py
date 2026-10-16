import os
import tempfile
import unittest
from typing import Optional

from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import DRAIN_TIMEOUT_DESTROY_AT_ONCE, DRAIN_TIMEOUT_NONE
from scaler.config.types.address import AddressConfig
from scaler.worker_manager.cloud.child_command import child_link_arguments, load_requirements_content


class TestLoadRequirementsContent(unittest.TestCase):
    def test_returns_literal_string(self):
        content = load_requirements_content("opengris-scaler>=1.0\nboto3\n")
        self.assertEqual(content, "opengris-scaler>=1.0\nboto3\n")

    def test_reads_valid_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("opengris-scaler\n")
            path = f.name
        try:
            content = load_requirements_content(path)
            self.assertEqual(content, "opengris-scaler\n")
        finally:
            os.unlink(path)

    def test_returns_literal_string_when_path_does_not_exist(self):
        content = load_requirements_content("/nonexistent/requirements.txt")
        self.assertEqual(content, "/nonexistent/requirements.txt")


class TestChildLinkArguments(unittest.TestCase):
    BOUND_ADDRESS = "tcp://172.31.8.78:8518"
    ADVERTISED_ADDRESS = "tcp://34.228.81.179:8518"

    def _config(self, advertised_children_address: Optional[str]) -> WorkerManagerConfig:
        return WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:8516"),
            worker_manager_id="wm-test",
            children_address=AddressConfig.from_string(self.BOUND_ADDRESS),
            advertised_children_address=(
                AddressConfig.from_string(advertised_children_address) if advertised_children_address else None
            ),
        )

    def test_units_dial_advertised_children_address(self):
        """Units behind NAT dial the advertised address, not the address the manager binds."""
        arguments = child_link_arguments(self._config(self.ADVERTISED_ADDRESS), "unit-1", 4)
        self.assertIn(f"--parent-address {self.ADVERTISED_ADDRESS} ", arguments)

    def test_units_dial_children_address_without_advertised_address(self):
        """Without an advertised address, units dial the address the manager binds."""
        arguments = child_link_arguments(self._config(None), "unit-1", 4)
        self.assertIn(f"--parent-address {self.BOUND_ADDRESS} ", arguments)

    @staticmethod
    def __child_drain_timeout(drain_timeout_seconds: int) -> str:
        config = WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:1"),
            worker_manager_id="parent",
            children_address=AddressConfig.from_string("tcp://127.0.0.1:2"),
            drain_timeout_seconds=drain_timeout_seconds,
        )
        return child_link_arguments(config, "unit-1", task_concurrency=4).split("--drain-timeout-seconds ")[1]

    def test_a_drain_without_a_deadline_or_without_a_drain_passes_to_the_child_unchanged(self):
        self.assertEqual(self.__child_drain_timeout(DRAIN_TIMEOUT_NONE), str(DRAIN_TIMEOUT_NONE))
        self.assertEqual(self.__child_drain_timeout(DRAIN_TIMEOUT_DESTROY_AT_ONCE), str(DRAIN_TIMEOUT_DESTROY_AT_ONCE))

    def test_the_child_ends_its_drain_before_the_parent_deadline(self):
        self.assertEqual(self.__child_drain_timeout(300), "270")

    def test_a_short_parent_deadline_still_gives_the_child_a_drain(self):
        """A child timeout of 0 would mean no drain at all, so a short parent timeout leaves the child at least 1s."""
        self.assertEqual(self.__child_drain_timeout(20), "1")
