import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from orb.sdk.exceptions import MethodExecutionError, NotFoundError

from scaler.worker_manager.cloud.orb_aws_ec2.worker_manager import ORBWorkerProvisioner

TEMPLATE_ID = "opengris-orb-unit-1"
INSTANCE_ID = "i-1"


class _FakeORB:
    """The ORB 1.8.5 SDK calls destroy_unit makes, as ORB answers them.

    A machine keeps its return_request_id once returned, and a second return of one machine is refused unless forced.
    Each return request ends with the next status in `return_outcomes`.
    """

    def __init__(self, return_outcomes: List[str], delete_template_failures: int = 0) -> None:
        self.machines: Dict[str, Dict[str, Optional[str]]] = {INSTANCE_ID: {"machine_id": INSTANCE_ID}}
        self.templates = {TEMPLATE_ID}
        self.request_statuses: Dict[str, str] = {}
        self.return_outcomes = list(return_outcomes)
        self.delete_template_failures = delete_template_failures

    async def get_machine(self, machine_id: str) -> Dict[str, Any]:
        if machine_id not in self.machines:
            raise NotFoundError("Machine", machine_id)
        return {key: value for key, value in self.machines[machine_id].items() if value is not None}

    async def create_return_request(self, machine_ids: List[str], force_return: bool = False) -> Dict[str, Any]:
        (machine_id,) = machine_ids
        if machine_id not in self.machines:
            raise NotFoundError("Machine", machine_id)
        machine = self.machines[machine_id]
        if machine.get("return_request_id") and not force_return:
            raise MethodExecutionError(
                f"Failed to execute create_return_request: Machine {machine_id} already has pending return request",
                method_name="create_return_request",
            )
        request_id = f"ret-{len(self.request_statuses) + 1}"
        self.request_statuses[request_id] = self.return_outcomes.pop(0)
        machine["return_request_id"] = request_id
        return {"created_request_ids": [request_id], "processed_machines": [machine_id], "skipped_machines": []}

    async def get_request_status(self, request_ids: List[str]) -> Dict[str, Any]:
        return {"requests": [{"request_id": request_ids[0], "status": self.request_statuses[request_ids[0]]}]}

    async def delete_template(self, template_id: str) -> Dict[str, Any]:
        if self.delete_template_failures > 0:
            self.delete_template_failures -= 1
            raise MethodExecutionError("Failed to execute delete_template: throttled", method_name="delete_template")
        if template_id not in self.templates:
            raise NotFoundError("Template", template_id)
        self.templates.remove(template_id)
        return {"template_id": template_id, "status": "deleted"}


def _provisioner(sdk: Any) -> ORBWorkerProvisioner:
    return ORBWorkerProvisioner(
        max_instances=-1, sdk=sdk, template_kwargs={}, user_data_for_unit=lambda unit_id: "", workers_per_instance=1
    )


class TestORBWorkerProvisioner(unittest.IsolatedAsyncioTestCase):
    async def test_create_unit_launches_one_instance_from_a_template_of_its_own(self) -> None:
        sdk = MagicMock()
        sdk.create_template = AsyncMock()
        sdk.validate_template = AsyncMock()
        sdk.create_request = AsyncMock(return_value={"created_request_id": "req-1"})
        sdk.get_request_status = AsyncMock(return_value={"requests": [{"status": "complete", "machine_ids": ["i-1"]}]})
        provisioner = ORBWorkerProvisioner(
            max_instances=-1,
            sdk=sdk,
            template_kwargs={"image_id": "ami-1"},
            user_data_for_unit=lambda unit_id: f"user data of {unit_id}",
            workers_per_instance=16,
        )

        with patch("asyncio.sleep", new_callable=AsyncMock):
            self.assertEqual(await provisioner.create_unit("unit-1"), ("opengris-orb-unit-1", "i-1"))
        self.assertEqual(sdk.create_template.call_args.kwargs["user_data"], "user data of unit-1")
        sdk.create_request.assert_awaited_once_with(template_id="opengris-orb-unit-1", count=1)
        self.assertEqual(provisioner.task_concurrency_per_unit(), 16)


class TestORBWorkerProvisionerDestroy(unittest.IsolatedAsyncioTestCase):
    """A destroy that failed part way resumes on retry, and never leaves a returned instance running."""

    async def __destroy(self, sdk: _FakeORB) -> None:
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await _provisioner(sdk).destroy_unit((TEMPLATE_ID, INSTANCE_ID))

    async def test_a_retry_after_a_failed_template_delete_completes_the_destroy(self) -> None:
        sdk = _FakeORB(return_outcomes=["complete"], delete_template_failures=1)

        with self.assertRaises(MethodExecutionError):
            await self.__destroy(sdk)
        await self.__destroy(sdk)

        self.assertEqual(sdk.templates, set())
        self.assertEqual(len(sdk.request_statuses), 1, "the instance is returned once")

    async def test_a_failed_return_is_returned_again_before_the_template_goes(self) -> None:
        sdk = _FakeORB(return_outcomes=["failed", "complete"])

        with self.assertRaises(RuntimeError):
            await self.__destroy(sdk)
        self.assertEqual(sdk.templates, {TEMPLATE_ID}, "the unit stays until its instance is gone")
        await self.__destroy(sdk)

        self.assertEqual(sdk.templates, set())
        self.assertEqual(list(sdk.request_statuses.values()), ["failed", "complete"])

    async def test_an_instance_orb_does_not_know_needs_no_return(self) -> None:
        sdk = _FakeORB(return_outcomes=[])
        sdk.machines.clear()

        await self.__destroy(sdk)

        self.assertEqual(sdk.templates, set())
