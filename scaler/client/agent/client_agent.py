import asyncio
import logging
import threading
from typing import Optional

from scaler.client.agent.disconnect_manager import ClientDisconnectManager
from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.agent.heartbeat_manager import ClientHeartbeatManager
from scaler.client.agent.object_manager import ClientObjectManager
from scaler.client.agent.task_manager import ClientTaskManager
from scaler.client.serializer.mixins import Serializer
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.message import (
    ClientDisconnect,
    ClientHeartbeatEcho,
    ClientShutdownResponse,
    GraphTask,
    GraphTaskCancel,
    ObjectInstruction,
    ObjectRequest,
    ObjectResponse,
    Task,
    TaskCancel,
    TaskResult,
)
from scaler.protocol.python.mixins import Message
from scaler.utility.event_loop import create_async_loop_routine
from scaler.utility.exceptions import ClientCancelledException, ClientQuitException, ClientShutdownException

from scaler.io.model import Session, ConnectorType, TcpAddr

class ClientAgent(threading.Thread):
    def __init__(
        self,
        identity: bytes,
        client_agent_address: TcpAddr,
        scheduler_address: TcpAddr,
        session: Session,
        future_manager: ClientFutureManager,
        stop_event: threading.Event,
        timeout_seconds: int,
        heartbeat_interval_seconds: int,
        serializer: Serializer,
    ):
        threading.Thread.__init__(self, daemon=True)

        self._stop_event = stop_event
        self._timeout_seconds = timeout_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._serializer = serializer

        self._identity = identity
        self._client_agent_address = client_agent_address
        self._scheduler_address = scheduler_address
        self._session = session

        self._future_manager = future_manager

        self._connector_internal = AsyncConnector(
            session=self._session,
            name="client_agent_internal",
            type_=ConnectorType.Pair,
            bind_or_connect="bind",
            address=self._client_agent_address,
            callback=self.__on_receive_from_client,
            identity=None,
        )

        print("client agent internal connected")

        self._connector_external = AsyncConnector(
            session=self._session,
            name="client_agent_external",
            type_=ConnectorType.Dealer,
            bind_or_connect="connect",
            address=self._scheduler_address,
            callback=self.__on_receive_from_scheduler,
            identity=self._identity,
        )

        self._disconnect_manager: Optional[ClientDisconnectManager] = None
        self._heartbeat_manager: Optional[ClientHeartbeatManager] = None
        self._task_manager: Optional[ClientTaskManager] = None

    def __initialize(self):
        self._disconnect_manager = ClientDisconnectManager()
        self._heartbeat_manager = ClientHeartbeatManager(death_timeout_seconds=self._timeout_seconds)
        self._object_manager = ClientObjectManager(identity=self._identity)
        self._task_manager = ClientTaskManager()

        # register all managers
        self._disconnect_manager.register(
            connector_internal=self._connector_internal, connector_external=self._connector_external
        )
        self._object_manager.register(
            connector_internal=self._connector_internal, connector_external=self._connector_external
        )
        self._task_manager.register(
            connector_external=self._connector_external,
            object_manager=self._object_manager,
            future_manager=self._future_manager,
        )
        self._heartbeat_manager.register(connector_external=self._connector_external)

    def __run_loop(self):
        self._loop = asyncio.new_event_loop()
        self._task = self._loop.create_task(self.__get_loops())
        self._loop.run_until_complete(self._task)
        self._loop.close()

    def run(self):
        self.__initialize()
        self.__run_loop()

    async def __on_receive_from_client(self, message: Message):
        if isinstance(message, ClientDisconnect):
            await self._disconnect_manager.on_client_disconnect(message)
            return

        if isinstance(message, ObjectInstruction):
            await self._object_manager.on_object_instruction(message)
            return

        if isinstance(message, ObjectRequest):
            await self._object_manager.on_object_request(message)
            return

        if isinstance(message, Task):
            await self._task_manager.on_new_task(message)
            return

        if isinstance(message, TaskCancel):
            await self._task_manager.on_cancel_task(message)
            return

        if isinstance(message, GraphTask):
            await self._task_manager.on_new_graph_task(message)
            return

        if isinstance(message, GraphTaskCancel):
            await self._task_manager.on_cancel_graph_task(message)
            return

        raise TypeError(f"Unknown {message=}")

    async def __on_receive_from_scheduler(self, message: Message):
        if isinstance(message, ClientShutdownResponse):
            await self._disconnect_manager.on_client_shutdown_response(message)
            return

        if isinstance(message, ClientHeartbeatEcho):
            await self._heartbeat_manager.on_heartbeat_echo(message)
            return

        if isinstance(message, TaskResult):
            await self._task_manager.on_task_result(message)
            return

        if isinstance(message, ObjectResponse):
            self._future_manager.on_object_response(message)
            return

        raise TypeError(f"Unknown {message=}")

    async def __get_loops(self):
        await self._heartbeat_manager.send_heartbeat()

        exception = None
        try:
            await asyncio.gather(
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._connector_internal.routine, 0),
                create_async_loop_routine(self._heartbeat_manager.routine, self._heartbeat_interval_seconds),
            )
        except BaseException as e:
            exception = e
        finally:
            self._stop_event.set()  # always set the stop event before setting futures' exceptions

            await self._object_manager.clear_all_objects(clear_serializer=True)

            self._connector_external.destroy()
            self._connector_internal.destroy()

        if exception is None:
            return

        if isinstance(exception, asyncio.CancelledError):
            logging.error("ClientAgent: async. loop cancelled")
            self._future_manager.set_all_futures_with_exception(ClientCancelledException("client cancelled"))
        elif isinstance(exception, (ClientQuitException, ClientShutdownException)):
            logging.info("ClientAgent: client quitting")
            self._future_manager.set_all_futures_with_exception(exception)
        elif isinstance(exception, TimeoutError):
            logging.error(f"ClientAgent: client timeout when connecting to {self._scheduler_address}")
            self._future_manager.set_all_futures_with_exception(exception)
        else:
            raise exception
