import logging
import os
import socket
import threading
import uuid
from typing import Optional

from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message

from scaler.io.model import ConnectorType, Session, Address, TCPAddress, IntraProcessAddress, Client, IntraProcessClient, TCPAddress, IntraProcessAddress


class SyncConnector:
    _client: Client | IntraProcessClient

    def __init__(self,
                 session: Session,
                 type_: ConnectorType,
                 address: Address,
                    identity: bytes | None):
        self._address = address

        match address:
            case TCPAddress():    
                self._client = Client(session, self._identity, type_)
                host = address.host
            case IntraProcessAddress():
                if type_ != ConnectorType.Pair:
                    raise ValueError(f"IntraProcessClient only supports pair type, got {type_}")

                self._client = IntraProcessClient(session, self._identity)
                host = address.name

        self._identity: bytes = (
            f"{os.getpid()}|{host}|{uuid.uuid4()}".encode()
            if identity is None
            else identity
        )

        self._client.connect(addr=self._address)
        self._lock = threading.Lock()

    def close(self):
        self._client.destroy()

    @property
    def address(self) -> Address:
        return self._address

    @property
    def identity(self) -> bytes:
        return self._identity

    def send(self, message: Message):
        with self._lock:
            self._client.send_sync(data=serialize(message))

    def receive(self) -> Optional[Message]:
        with self._lock:
            msg = self._client.recv_sync()

        return self.__compose_message(msg.payload)

    def __compose_message(self, payload: bytes) -> Optional[Message]:
        result: Optional[Message] = deserialize(payload)
        if result is None:
            logging.error(f"{self.__get_prefix()}: received unknown message: {payload!r}")
            return None

        return result

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
