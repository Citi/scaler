import logging
import os
import uuid
from collections import defaultdict

from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message
from scaler.protocol.python.status import BinderStatus
from scaler.utility.mixins import Looper, Reporter

from scaler.io.model import BinderCallback, NetworkConnector, ConnectorType, Session, TCPAddress


class AsyncBinder(Looper, Reporter):
    _client: NetworkConnector
    _identity: bytes
    _callback: BinderCallback | None
    _received: dict[str, int]
    _sent: dict[str, int]

    def __init__(self, session: Session, name: str, address: TCPAddress, identity: bytes | None = None) -> None:
        if identity is None:
            identity = f"{os.getpid()}|{name}|{uuid.uuid4()}".encode()
        self._identity = identity

        self._callback = None
        self._received = defaultdict(lambda: 0)
        self._sent = defaultdict(lambda: 0)

        self._connector = NetworkConnector(session, self._identity, ConnectorType.Router)
        self._connector.bind(addr=address)

    def destroy(self):
        self._connector.destroy()

    def register(self, callback: BinderCallback) -> None:
        self._callback = callback

    async def send(self, to: bytes, message: Message) -> None:
        self.__count_sent(message.__class__.__name__)
        await self._connector.send(to=to, data=serialize(message))

    async def routine(self) -> None:
        client_msg = await self._connector.recv()
        message = deserialize(client_msg.payload)

        if message is None:
            logging.error(f"received unknown message from {client_msg.address!r}: {client_msg.payload!r}")
            return

        self.__count_received(message.__class__.__name__)
        await self._callback(client_msg.address, message)

    def get_status(self) -> BinderStatus:
        return BinderStatus.new_msg(received=self._received, sent=self._sent)

    def __count_received(self, message_type: str) -> None:
        self._received[message_type] += 1

    def __count_sent(self, message_type: str) -> None:
        self._sent[message_type] += 1

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
