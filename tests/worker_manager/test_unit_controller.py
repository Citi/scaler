import asyncio
import unittest
from typing import Dict, List, Set

from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager.mixins import UnitHandle, UnitProvisioner
from scaler.worker_manager.unit_controller import UnitController, UnitState
from tests.utility.utility import logging_test_name

TASK_CONCURRENCY_PER_UNIT = 2
RESTART_BACKOFF_SECONDS = 60


class _FakeProvisioner(UnitProvisioner):
    """Units that exist until destroyed; each create waits for `release_creates` when `hold_creates` is set."""

    def __init__(self, max_units: int = -1) -> None:
        self._max_units = max_units
        self.existing: Set[str] = set()
        self.created: List[str] = []
        self.destroyed: List[str] = []
        self.fail_creates = False
        self.fail_destroys = False
        self.hold_creates = False
        self.release_creates = asyncio.Event()

    async def create_unit(self, unit_id: str) -> UnitHandle:
        if self.hold_creates:
            await self.release_creates.wait()
        if self.fail_creates:
            raise RuntimeError("no capacity")
        self.created.append(unit_id)
        self.existing.add(unit_id)
        return unit_id

    async def destroy_unit(self, handle: UnitHandle) -> None:
        if self.fail_destroys:
            raise RuntimeError("api error")
        assert isinstance(handle, str)
        self.destroyed.append(handle)
        self.existing.discard(handle)

    async def poll_units(self, handles: Set[UnitHandle]) -> Set[UnitHandle]:
        return {handle for handle in handles if handle in self.existing}

    def max_units(self) -> int:
        return self._max_units

    def task_concurrency_per_unit(self) -> int:
        return TASK_CONCURRENCY_PER_UNIT

    def poll_interval_seconds(self) -> int:
        return 1


class TestUnitController(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.provisioner = _FakeProvisioner()
        self.controller = self.__make_controller(self.provisioner)

    @staticmethod
    def __make_controller(provisioner: UnitProvisioner, scale_down_cooldown_seconds: float = 0) -> UnitController:
        return UnitController(
            provisioner,
            scale_down_cooldown_seconds=scale_down_cooldown_seconds,
            restart_backoff_seconds=RESTART_BACKOFF_SECONDS,
        )

    async def __tick(self, controller: UnitController) -> None:
        """Run one routine, then let the provisioner calls it started finish."""
        await controller.routine()
        for _ in range(3):
            await asyncio.sleep(0)

    def __states(self, controller: UnitController) -> Dict[UnitState, int]:
        states: Dict[UnitState, int] = {}
        for unit in controller._units.values():
            states[unit.state] = states.get(unit.state, 0) + 1
        return states

    async def test_creates_enough_units_for_the_desired_task_concurrency(self) -> None:
        self.controller.set_desired_task_concurrency(5)
        await self.__tick(self.controller)
        self.assertEqual(len(self.provisioner.created), 3)

    async def test_max_units_caps_the_fleet(self) -> None:
        provisioner = _FakeProvisioner(max_units=2)
        controller = self.__make_controller(provisioner)
        controller.set_desired_task_concurrency(100)
        await self.__tick(controller)
        self.assertEqual(len(provisioner.created), 2)

    async def test_a_create_in_flight_counts_as_supply(self) -> None:
        """A slow create does not make the next routine create another unit."""
        self.provisioner.hold_creates = True
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        await self.__tick(self.controller)
        self.assertEqual(self.__states(self.controller), {UnitState.pending: 1})

        self.provisioner.release_creates.set()
        await self.__tick(self.controller)
        await self.__tick(self.controller)
        self.assertEqual(self.provisioner.created, list(self.controller._units))
        self.assertEqual(self.__states(self.controller), {UnitState.active: 1})

    async def test_a_unit_that_exists_becomes_active(self) -> None:
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        self.assertEqual(self.__states(self.controller), {UnitState.pending: 1})
        await self.__tick(self.controller)
        self.assertEqual(self.__states(self.controller), {UnitState.active: 1})

    async def test_a_lost_unit_is_removed_and_replaced_after_the_backoff(self) -> None:
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        await self.__tick(self.controller)
        (lost_unit_id,) = self.provisioner.existing
        self.provisioner.existing.clear()

        await self.__tick(self.controller)
        self.assertNotIn(lost_unit_id, self.controller._units)
        self.assertEqual(self.provisioner.created, [lost_unit_id], "the backoff holds the replacement back")

        self.controller._create_not_before = 0
        await self.__tick(self.controller)
        self.assertEqual(len(self.provisioner.created), 2)

    async def test_a_failed_create_leaves_no_unit_and_arms_the_backoff(self) -> None:
        self.provisioner.fail_creates = True
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        self.assertEqual(self.controller._units, {})

        self.provisioner.fail_creates = False
        await self.__tick(self.controller)
        self.assertEqual(self.provisioner.created, [])

    async def test_scale_down_destroys_the_excess_units(self) -> None:
        self.controller.set_desired_task_concurrency(3 * TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        await self.__tick(self.controller)

        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        self.assertEqual(len(self.provisioner.destroyed), 2)
        self.assertEqual(self.__states(self.controller), {UnitState.active: 1})

    async def test_scale_down_waits_for_the_cooldown(self) -> None:
        controller = self.__make_controller(self.provisioner, scale_down_cooldown_seconds=60)
        controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(controller)
        await self.__tick(controller)

        controller.set_desired_task_concurrency(0)
        await self.__tick(controller)
        self.assertEqual(self.provisioner.destroyed, [])

    async def test_a_failed_destroy_is_retried(self) -> None:
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        await self.__tick(self.controller)

        self.provisioner.fail_destroys = True
        self.controller.set_desired_task_concurrency(0)
        await self.__tick(self.controller)
        self.assertEqual(self.__states(self.controller), {UnitState.stopping: 1})

        self.provisioner.fail_destroys = False
        await self.__tick(self.controller)
        self.assertEqual(self.controller._units, {})
        self.assertEqual(len(self.provisioner.destroyed), 1)

    async def test_terminate_destroys_every_unit_including_one_still_being_created(self) -> None:
        self.controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)
        await self.__tick(self.controller)

        self.provisioner.hold_creates = True
        self.controller.set_desired_task_concurrency(2 * TASK_CONCURRENCY_PER_UNIT)
        await self.__tick(self.controller)

        terminate = asyncio.ensure_future(self.controller.terminate())
        await asyncio.sleep(0)
        self.provisioner.release_creates.set()
        await terminate

        self.assertEqual(sorted(self.provisioner.destroyed), sorted(self.provisioner.created))
        self.assertEqual(len(self.provisioner.destroyed), 2)
        self.assertEqual(self.controller._units, {})
