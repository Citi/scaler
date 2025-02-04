import logging
import os
import socket
import threading
import uuid
from typing import Optional

from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message

from scaler.io.model import ConnectorType, Session, Addr, TcpAddr, InprocAddr, Client, InprocClient, TcpAddr, InprocAddr


class SyncConnector:
    _client: Client | InprocClient

    def __init__(self,
                 session: Session,
                 type_: ConnectorType,
                 address: Addr,
                    identity: bytes | None):
        self._address = address

        match address:
            case TcpAddr():    
                host = address.host
            case InprocAddr():
                host = address.name

        self._identity: bytes = (
            f"{os.getpid()}|{host}|{uuid.uuid4()}".encode()
            if identity is None
            else identity
        )

        match address:
            case TcpAddr():
                self._client = Client(session, self._identity, type_)
                self._client.connect(addr=self._address)
                host = address.host
            case InprocAddr():
                if type_ != ConnectorType.Pair:
                    raise ValueError(f"Inproc only supports pair type, got {type_}")

                self._client = InprocClient(session, self._identity)
                self._client.connect(addr=address.name)
                host = address.name

        self._lock = threading.Lock()

    def close(self):
        ...
        # self._socket.close()

    @property
    def address(self) -> Addr:
        return self._address

    @property
    def identity(self) -> bytes:
        return self._identity

    def send(self, message: Message):
        with self._lock:
            match self._client:
                case Client():
                    self._client.send_sync(data=serialize(message))
                case InprocClient():
                    self._client.send(data=serialize(message))

    def receive(self) -> Optional[Message]:
        with self._lock:
            match self._client:
                case Client():
                    msg = self._client.recv_sync()
                case InprocClient():
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
