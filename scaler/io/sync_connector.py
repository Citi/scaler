import logging
import os
import socket
import threading
import uuid
from typing import Optional

from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message

from scaler.io.model import ConnectorType, Session, Address, TCPAddress, IntraProcessAddress, InterProcessAddress, Connector, TCPAddress, IntraProcessAddress


class SyncConnector:
    _connector: Connector

    def __init__(self,
                 session: Session,
                 type_: ConnectorType,
                 address: Address,
                    identity: bytes | None):
        self._address = address

        match address:
            case TCPAddress():    
                host = address.host
            case IntraProcessAddress():
                host = address.name
            case InterProcessAddress():
                host = address.path

        self._identity: bytes = (
            f"{os.getpid()}|{host}|{uuid.uuid4()}".encode()
            if identity is None
            else identity
        )

        self._connector = Connector(session, self._identity, type_, address.protocol)
        self._connector.connect(addr=self._address)
        self._lock = threading.Lock()

    def close(self):
        self._connector.destroy()

    @property
    def address(self) -> Address:
        return self._address

    @property
    def identity(self) -> bytes:
        return self._identity

    def send(self, message: Message):
        with self._lock:
            self._connector.send_sync(data=serialize(message))

    def receive(self) -> Optional[Message]:
        with self._lock:
            msg = self._connector.recv_sync()

        return self.__compose_message(msg.payload)

    def __compose_message(self, payload: bytes) -> Optional[Message]:
        result: Optional[Message] = deserialize(payload)
        if result is None:
            logging.error(f"{self.__get_prefix()}: received unknown message: {payload!r}")
            return None

        return result

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
