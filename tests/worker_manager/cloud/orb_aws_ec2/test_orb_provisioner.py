import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.worker_manager.cloud.orb_aws_ec2.worker_manager import ORBWorkerProvisioner


class TestORBWorkerProvisioner(unittest.IsolatedAsyncioTestCase):
    async def test_create_unit_requests_one_instance_and_returns_its_id(self) -> None:
        sdk = MagicMock()
        sdk.create_request = AsyncMock(return_value={"created_request_id": "req-1"})
        sdk.get_request_status = AsyncMock(return_value={"requests": [{"status": "complete", "machine_ids": ["i-1"]}]})
        provisioner = ORBWorkerProvisioner(max_instances=-1, sdk=sdk, template_id="tmpl", workers_per_instance=16)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            self.assertEqual(await provisioner.create_unit("unit-1"), "i-1")
        sdk.create_request.assert_awaited_once_with(template_id="tmpl", count=1)
        self.assertEqual(provisioner.task_concurrency_per_unit(), 16)
