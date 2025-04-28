__ALL__ = [
    "Session",
    "Message",
    "Connector",
    "ConnectorType",
    "TcpAddr",
    "InprocAddr",
    "Addr",
    "Protocol",
]

from scaler.io.cpp.ffi import FFITypes, ffi, lib as C, c_async, Message
from scaler.io.cpp.errors import check_status

from enum import IntEnum, unique
from abc import ABC, abstractmethod


class Session:
    _obj: "FFITypes.CData"
    _clients: list
    _destroyed: bool = False

    def __init__(self, io_threads: int) -> None:
        self._obj = ffi.new("struct Session *")
        check_status(
            C.session_init(self._obj, io_threads)
        )

        self._clients = []

    def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True

        for client in self._clients:
            client.destroy()

        check_status(
            C.session_destroy(self._obj, True)
        )

    @property
    def destroyed(self) -> bool:
        return self._destroyed

    def register_client(self, client) -> None:
        self._clients.append(client)

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        return

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

class Connector:
    _obj: "FFITypes.CData"
    _destroyed: bool = False
    _session: Session

    def __init__(self, session: Session, identity: bytes, type_: ConnectorType, protocol: Protocol):
        if session.destroyed:
            raise RuntimeError("session is destroyed")

        self._obj = ffi.new("struct Connector *")
        check_status(
            C.connector_init(session._obj, self._obj, protocol.value, type_.value, identity, len(identity))
        )

        self._session = session
        self._session.register_client(self)

    def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True

        check_status(
            C.connector_destroy(self._obj, True)
        )

        print(f"destroyed!")

        import traceback
        traceback.print_stack()

    def __check_destroyed(self) -> None:
        if self._destroyed:
            raise RuntimeError("client is destroyed")
        
    def bind(self, addr: Address) -> None:
        self.__check_destroyed()

        match addr:
            case TCPAddress():
                host, port = addr.host, addr.port
            case InterProcessAddress():
                host, port = addr.path, 0
            case IntraProcessAddress():
                host, port = addr.name, 0

        check_status(
            C.connector_bind(self._obj, host.encode(), port)
        )

    def connect(self, addr: Address) -> None:
        self.__check_destroyed()

        match addr:
            case TCPAddress():
                host, port = addr.host, addr.port
            case InterProcessAddress():
                host, port = addr.path, 0
            case IntraProcessAddress():
                host, port = addr.name, 0

        check_status(
            C.connector_connect(self._obj, host.encode(), port)
        )

    async def send(self, to: bytes | None = None, data: bytes | None = None) -> None:
        self.__check_destroyed()

        if to is None:
            to, to_len = ffi.NULL, 0
        else:
            to_len = len(to)

        await c_async(C.connector_send_async, self._obj, to, to_len, data, len(data))

    def send_sync(self, to: bytes | None = None, data: bytes | None = None) -> None:
        self.__check_destroyed()

        if to is None:
            to, to_len = ffi.NULL, 0
        else:
            to_len = len(to)

        check_status(
            C.connector_send_sync(self._obj, to, to_len, data, len(data))
        )

    async def recv(self) -> Message:
        self.__check_destroyed()

        return await c_async(C.connector_recv_async, self._obj)
    
    def recv_sync(self) -> Message:
        self.__check_destroyed()

        msg = ffi.new("struct Message *")
        check_status(
            C.connector_recv_sync(self._obj, msg)
        )

        # copy the message
        msg_ = Message(msg)

        # free data
        C.message_destroy(msg)
        return msg_
