import ipaddress
import unittest
from unittest import mock

from scaler.utility.network_util import get_hostname


class TestGetHostname(unittest.TestCase):
    def setUp(self) -> None:
        get_hostname.cache_clear()
        self.addCleanup(get_hostname.cache_clear)

    def test_returns_hostname(self) -> None:
        """A real hostname is reported as is."""
        with mock.patch("socket.gethostname", return_value="worker-host"):
            self.assertEqual(get_hostname(), "worker-host")

    def test_localhost_reports_ip(self) -> None:
        """A host named localhost, as in an OCI container instance, reports its non-loopback IP instead."""
        for hostname in ("localhost", "localhost.localdomain", ""):
            with self.subTest(hostname=hostname), mock.patch("socket.gethostname", return_value=hostname):
                get_hostname.cache_clear()
                self.assertFalse(ipaddress.ip_address(get_hostname()).is_loopback)

    def test_localhost_without_route_keeps_hostname(self) -> None:
        """Without a route to pick an IP from, the hostname is reported as is."""
        with (
            mock.patch("socket.gethostname", return_value="localhost"),
            mock.patch("socket.socket.connect", side_effect=OSError("network is unreachable")),
        ):
            self.assertEqual(get_hostname(), "localhost")


if __name__ == "__main__":
    unittest.main()
