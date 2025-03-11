__ALL__ = [
    "Session",
    "Client",
    "Message",
    "Callback",
    "ConnectorType",
    "TcpAddr",
    "InprocAddr",
    "Addr",
    "Protocol",
    "InprocClient",
]

import sys
from os import path

sys.path.append(path.join(path.dirname(__file__), "cpp"))
from ffi import FFITypes, ffi, lib as C, c_async, Message

sys.path.pop()

from enum import IntEnum, unique
from abc import ABC, abstractmethod
from typing import Awaitable, Callable, TypeAlias


class Session:
    _obj: "FFITypes.CData"
    _clients: list
    _destroyed: bool

    def __init__(self, io_threads: int) -> None:
        self._obj = ffi.new("struct Session *")
        C.session_init(self._obj, io_threads)

        self._destroyed = False
        self._clients = []

    def __del__(self) -> None:
        self.destroy()

    def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True

        for client in self._clients:
            client.destroy()

        C.session_destroy(self._obj)

    def register_client(self, client) -> None:
        self._clients.append(client)

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        return


BinderCallback: TypeAlias = Callable[[bytes, Message], Awaitable[None]]
ConnectorCallback: TypeAlias = Callable[[Message], Awaitable[None]]


@unique
class ConnectorType(IntEnum):
    Pair = C.Pair
    Pub = C.Pub
    Sub = C.Sub
    Dealer = C.Dealer
    Router = C.Router


@unique
class Protocol(IntEnum):
    TCP = C.TCP
    IntraProcess = C.IntraProcess
    InterProcess = C.InterProcess


class Address(ABC):
    @property
    @abstractmethod
    def protocol(self) -> Protocol: ...

    @staticmethod
    def from_str(addr: str) -> "Address":
        protocol, addr = addr.split("://")

        match protocol:
            case "tcp":
                addr, port = addr.split(":")
                return TCPAddress(host=addr, port=int(port))
            case "intraprocess":
                return IntraProcessAddress(name=addr)
            case "interprocess":
                return InterProcessAddress(path=addr)
            case _:
                raise ValueError(f"unknown protocol: {protocol}")


class TCPAddress(Address):
    __match_args__ = ("host", "port")

    host: str
    port: int

    def __init__(self, host: str, port: int):
        if not isinstance(host, str):
            raise TypeError(f"host must be a string; is {type(host)}")

        if not isinstance(port, int):
            raise TypeError(f"port must be an integer; is {type(port)}")

        self.host = host
        self.port = port

    def __str__(self) -> str:
        return f"tcp://{self.host}:{self.port}"

    def copywith(self, host: str | None = None, port: int | None = None) -> "TCPAddress":
        return TCPAddress(host=host or self.host, port=port or self.port)

    @property
    def protocol(self) -> Protocol:
        return Protocol.TCP

    @staticmethod
    def bindall(port: int) -> "TCPAddress":
        if not isinstance(port, int):
            raise TypeError(f"port must be an integer; is {type(port)}")

        return TCPAddress(host="*", port=port)

    @staticmethod
    def localhost(port: int) -> "TCPAddress":
        if not isinstance(port, int):
            raise TypeError(f"port must be an integer; is {type(port)}")

        return TCPAddress(host="127.0.0.1", port=port)

    def from_str(addr: str) -> "TCPAddress":
        addr = Address.from_str(addr)

        if not isinstance(addr, TCPAddress):
            raise ValueError(f"expected a tcp address, got: {addr}")

        return addr


class IntraProcessAddress(Address):
    __match_args__ = ("name",)

    name: str

    def __init__(self, name: str):
        self.name = name

    def __str__(self) -> str:
        return f"intraprocess://{self.name}"

    @property
    def protocol(self) -> Protocol:
        return Protocol.IntraProcess


class InterProcessAddress(Address):
    __match_args__ = ("path",)

    path: str

    def __init__(self, path: str):
        self.path = path

    def __str__(self) -> str:
        return f"interprocess://{self.path}"

    @property
    def protocol(self) -> Protocol:
        return Protocol.InterProcess


class IntraProcessClient:
    _obj: "FFITypes.CData"
    _destroyed: bool

    def __init__(self, session: Session, identity: bytes):
        self._obj = ffi.new("struct IntraProcessClient *")
        C.intraprocess_init(session._obj, self._obj, identity, len(identity))

        session.register_client(self)
        self._destroyed = False

    def __del__(self):
        self.destroy()

    def destroy(self) -> None:
        if self._destroyed:
            return

        C.intraprocess_destroy(self._obj)

    def __check_destroyed(self) -> None:
        if self._destroyed:
            raise RuntimeError("client is destroyed")

    def bind(self, addr: str) -> None:
        self.__check_destroyed()

        match addr:
            case IntraProcessAddress(name):
                pass
            case str(name):
                pass
            case _:
                raise ValueError(f"addr must be str or IntraProcessAddress; got: {type(addr)}")

        C.intraprocess_bind(self._obj, name.encode(), len(name))

    def connect(self, addr: str | IntraProcessAddress) -> None:
        self.__check_destroyed()

        match addr:
            case IntraProcessAddress(name):
                pass
            case str(name):
                pass
            case _:
                raise ValueError(f"addr must be str or IntraProcessAddress; got: {type(addr)}")
            
        C.intraprocess_connect(self._obj, name.encode(), len(name))

    def send_sync(self, data: bytes) -> None:
        self.__check_destroyed()
        C.intraprocess_send(self._obj, data, len(data))

    def recv_sync(self) -> Message:
        self.__check_destroyed()

        msg = ffi.new("struct Message *")
        C.intraprocess_recv_sync(self._obj, msg)
        msg_ = Message(msg)
        C.message_destroy(msg)
        return msg_

    async def recv(self) -> Message:
        return await c_async(C.intraprocess_recv_async, self._obj)

def easy_hash(b: bytes) -> str:
    import hashlib
    return hashlib.md5(b, usedforsecurity=False).hexdigest()[:8]

class Client:
    _obj: "FFITypes.CData"
    _destroyed: bool

    def __init__(self, session: Session, identity: bytes, type_: ConnectorType):
        self._obj = ffi.new("struct Client *")
        C.client_init(session._obj, self._obj, Protocol.TCP, identity, len(identity), type_.value)

        session.register_client(self)
        self._destroyed = False

        # todo: remove after testing
        self.identity = identity

    def __del__(self):
        self.destroy()

    def destroy(self) -> None:
        if self._destroyed:
            return

        C.client_destroy(self._obj)
        self._destroyed = True

    def __check_destroyed(self) -> None:
        if self._destroyed:
            raise RuntimeError("client is destroyed")

    def bind(self, host: str | None = None, port: int | None = None, addr: TCPAddress | None = None) -> None:
        self.__check_destroyed()

        match (host, port, addr):
            case (None, None, TCPAddress(host, port)):
                ...
            case (str(host), int(port), None):
                ...
            case _:
                raise ValueError("either addr or host and port must be provided")

        C.client_bind(self._obj, host.encode(), port)

    def connect(self, host: str | None = None, port: int | None = None, addr: TCPAddress | None = None) -> None:
        self.__check_destroyed()

        match (host, port, addr):
            case (None, None, TCPAddress(host, port)):
                ...
            case (str(host), int(port), None):
                ...
            case _:
                raise ValueError("either addr or host and port must be provided")

        C.client_connect(self._obj, host.encode(), port)

    async def send(self, to: bytes | None = None, data: bytes | None = None, msg: Message | None = None) -> None:
        self.__check_destroyed()

        match (to, data, msg):
            case (None, None, Message(to, data)):
                ...
            case (bytes(), bytes(), None):
                ...
            case (None, bytes(), None):
                ...
            case _:
                raise ValueError(f"either msg or to and data must be provided, got: to={to}, data={data}, msg={msg}")

        if to is None:
            to = ffi.NULL
            to_len = 0
        else:
            to_len = len(to)

        # print(f"SEND: {easy_hash(data)}")

        await c_async(C.client_send, self._obj, to, to_len, data, len(data))

    def send_sync(self, to: bytes | None = None, data: bytes | None = None, msg: Message | None = None) -> None:
        self.__check_destroyed()

        match (to, data, msg):
            case (None, None, Message(to, data)):
                ...
            case (bytes(), bytes(), None):
                ...
            case (None, bytes(), None):
                ...
            case _:
                raise ValueError("either msg or to and data must be provided")

        if to is None:
            to = ffi.NULL
            to_len = 0
        else:
            to_len = len(to)

        C.client_send_sync(self._obj, to, to_len, data, len(data))

    def recv_sync(self) -> Message:
        self.__check_destroyed()

        msg = ffi.new("struct Message *")
        C.client_recv_sync(self._obj, msg)

        # copy the message
        msg_ = Message(msg)

        # free data
        C.message_destroy(msg)
        return msg_

    async def recv(self) -> Message:
        self.__check_destroyed()
        msg_: Message = await c_async(C.client_recv, self._obj)
        # print(f"RECV: {easy_hash(msg_.payload)}")
        return msg_
