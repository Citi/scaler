import logging
import os
import uuid
from typing import Literal


from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message

from scaler.io.model import ConnectorCallback, Client, ConnectorType, Session, TcpAddr


class AsyncConnector:
    _client: Client
    _address: TcpAddr
    _identity: bytes
    _callback: ConnectorCallback | None

    def __init__(
        self,
        session: Session,
        name: str,
        type_: ConnectorType,
        address: TcpAddr,
        bind_or_connect: Literal["bind", "connect"],
        callback: ConnectorCallback | None,
        identity: bytes | None,
    ):
        if identity is None:
            identity = f"{os.getpid()}|{name}|{uuid.uuid4().bytes.hex()}".encode()
        self._identity = identity

        self._address = address
        self._callback = callback
        self._client = Client(session, self._identity, type_)

        match bind_or_connect:
            case "bind":
                self._client.bind(addr=self._address)
            case "connect":
                self._client.connect(addr=self._address)
            case _:
                raise TypeError("bind_or_connect has to be 'bind' or 'connect'")
            
    def destroy(self):
        self._client.destroy()

    @property
    def address(self) -> str:
        return str(self._address)

    @property
    def identity(self) -> bytes:
        return self._identity

    async def routine(self) -> None:
        if self._callback is None:
            return

        client_msg = await self._client.recv()
        message = deserialize(client_msg.payload)

        if message is None:
            logging.error(f"received unknown message: {client_msg.payload!r}")
            return

        await self._callback(message)


    async def send(self, message: Message) -> None:
        await self._client.send(data=serialize(message))

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
