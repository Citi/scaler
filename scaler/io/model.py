__ALL__ = ["Session", "Client", "Message", "Callback", "ConnectorType", "TcpAddr", "InprocAddr", "Addr", "Protocol", "InprocClient"]

import sys
from os import path
sys.path.append(path.join(path.dirname(__file__), "cpp"))
from ffi import FFITypes, ffi, lib as C, c_async, c_async_wrapper
sys.path.pop()

import asyncio
from enum import IntEnum, unique
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Awaitable, Callable, TypeAlias

@dataclass
class BorrowedMessage:
    _payload_buffer: "FFITypes.buffer"
    _payload: bytes | None
    _address: bytes

    def __init__(self, obj: "FFITypes.CData"): # Message*
        # the message owns the address and it must be freed when we're done with it
        self._payload_buffer = ffi.buffer(ffi.gc(obj.payload.data, C.free), obj.payload.len)
        self._payload = None

        # copy the address
        self._address = bytes(ffi.buffer(obj.address.data, obj.address.len))

    @property
    def payload(self) -> bytes:
        if self._payload is None:
            self._payload = bytes(self._payload_buffer)
        return self._payload

    @property
    def address(self) -> bytes:
        return self._address

# this is called from C to inform the asyncio runtime that a future was completed
@ffi.def_extern()
def future_set_result(future_handle: "FFITypes.CData", result: "FFITypes.CData") -> None:
    if result == ffi.NULL:
        result = None
    else:
        msg = ffi.cast("struct Message *", result)

        result = BorrowedMessage(msg)

    future: asyncio.Future = ffi.from_handle(future_handle)

    # using `call_soon_threadsafe()` is very important:
    # - https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.call_soon_threadsafe
    future.get_loop().call_soon_threadsafe(future.set_result, result)

class Session:
    _obj: "FFITypes.CData"
    _clients: list = []
    _destroyed: bool = False

    def __init__(self, io_threads: int) -> None:
        self._obj = ffi.new("struct Session *")
        C.session_init(self._obj, io_threads)

        self._destroyed = False

    def __del__(self) -> None:
        self.destroy()

    def destroy(self) -> None:
        for client in self._clients:
            client.destroy()

        if self._destroyed:
            return

        C.session_destroy(self._obj)
        self._destroyed = True

    def register_client(self, client) -> None:
        self._clients.append(client)

    def __enter__(self) -> "Session":
        return self
    
    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        self.__del__()

@dataclass
class Message:
    __match_args__ = ("addr", "payload")

    addr: bytes | None
    payload: bytes

    @property
    def has_source(self) -> bool:
        return self.addr is not None


BinderCallback: TypeAlias = Callable[[bytes, BorrowedMessage], Awaitable[None]]
ConnectorCallback: TypeAlias = Callable[[BorrowedMessage], Awaitable[None]]

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

    def __init__(self, session: Session, identity: bytes):
        self._obj = ffi.new("struct IntraProcessClient *")
        C.intraprocess_init(session._obj, self._obj, identity, len(identity))

        session.register_client(self)

    def destroy(self) -> None:
        ... # TODO

    def bind(self, addr: str) -> None:
        C.intraprocess_bind(self._obj, addr.encode(), len(addr))

    def connect(self, addr: str) -> None:
        C.intraprocess_connect(self._obj, addr.encode(), len(addr))

    def send(self, data: bytes) -> None:
        C.intraprocess_send(self._obj, data, len(data))

    def recv_sync(self) -> BorrowedMessage:
        msg = ffi.new("struct Message *")
        C.intraprocess_recv_sync(self._obj, msg)

        return BorrowedMessage(msg)

    async def recv(self) -> BorrowedMessage:
        intraprocess_recv = c_async_wrapper(C.intraprocess_recv_async)
        return await intraprocess_recv(self._obj)

class Client:
    _obj: "FFITypes.CData"
    _destroyed: bool = False

    def __init__(self, session: Session, identity: bytes, type_: ConnectorType):
        self._obj = ffi.new("struct Client *")
        C.client_init(session._obj, self._obj, Protocol.TCP, identity, len(identity), type_.value)

        session.register_client(self)

    def __del__(self):
        self.destroy()

    def destroy(self) -> None:
        if self._destroyed:
            return
        
        C.client_destroy(self._obj)
        self._destroyed = True

    def __check_destroyed(self) -> None:
        if self._destroyed:
            raise ValueError("client is destroyed")

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

        return await c_async(C.client_send, self._obj, to, to_len, data, len(data))
    
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

    def recv_sync(self) -> BorrowedMessage:
        self.__check_destroyed()

        msg = ffi.new("struct Message *")
        msg.payload.data = ffi.NULL
        C.client_recv_sync(self._obj, msg)
        return BorrowedMessage(msg)

    async def recv(self) -> BorrowedMessage:
        self.__check_destroyed()

        return await c_async(C.client_recv, self._obj)

# if __name__ == "__main__":
#     import asyncio, json

#     port = 5564
#     session = Session(2)
#     router = Client(session, b"router", ConnectorType.Router)
#     router.bind(addr=TcpAddr.bindall(port))

#     c2 = Client(session, b"client-1", ConnectorType.Pair)
#     c2.connect(addr=TcpAddr.localhost(port))

#     c3 = Client(session, b"client-2", ConnectorType.Pair)
#     c3.connect(addr=TcpAddr.localhost(port))

#     async def router_routine():
#         import random

#         reqs = {}

#         while True:
#             msg = await router.recv()
#             data = json.loads(msg.payload)

#             match data["method"]:
#                 case "ready":
#                     cmd = f"{random.randint(1, 100)} ** 3"
#                     reqs[msg.addr] = cmd
#                     await router.send(to=msg.addr, data=cmd.encode())
#                 case "result":
#                     print(f"[{msg.addr.decode()}]: {reqs[msg.addr]} = {data['result']}")
#                 case "done":
#                     del reqs[msg.addr]

#                     if not reqs:
#                         return
#                 case _:
#                     print("unknown method")

#     async def client_routine(client: Client, mult):
#         counter = 0

#         while True:
#             await client.send(data=json.dumps({"method": "ready"}).encode())

#             msg = await client.recv()
#             cmd = msg.payload.decode()
#             result = eval(cmd)

#             await client.send(data=json.dumps({"method": "result", "result": result}).encode())

#             counter += 1

#             if counter > 2:
#                 break

#             await asyncio.sleep(counter * mult)

#         await client.send(data=json.dumps({"method": "done"}).encode())


#     async def main():
#         t1 = asyncio.create_task(router_routine())
#         t2 = asyncio.create_task(client_routine(c2, 1))
#         t3 = asyncio.create_task(client_routine(c3, 1.5))

#         await t1
#         await t2
#         await t3
    
#     asyncio.run(main())

if __name__ == "__main__":
    session = Session(1)
    router = Client(session, b"router", ConnectorType.Router)
    router.bind(addr=TCPAddress.bindall(5564))

    c2 = Client(session, b"client-1", ConnectorType.Pair)
    c2.connect(addr=TCPAddress.localhost(5564))

    print("A")

    c3 = Client(session, b"client-2", ConnectorType.Pair)
    c3.connect(addr=TCPAddress.localhost(5564))

    print("B")

    router.send_sync(to=b"client-1", data=b"hello")
    router.send_sync(to=b"client-1", data=b"hello")
    router.send_sync(to=b"client-1", data=b"hello")
    # router.send_sync(to=b"client-1", data=("a"*1024*1000).encode())
    router.send_sync(to=b"client-1", data=b"bye")
    router.send_sync(to=b"client-2", data=b"world")

    msg = c3.recv_sync()
    print(msg)

    while True:
        msg = c2.recv_sync()

        print(msg)

        if msg.payload == b"bye":
            break


# if __name__ == "__main__":
#     import asyncio

#     session = Session(1)
#     addr = InprocAddr("test")

#     p1 = InprocClient(session, b"p1")
#     p1.bind("test")
#     p2 = InprocClient(session, b"p2")
#     p2.connect("test")

#     p2.send(b"hello")
#     p1.send(b"world")

#     async def main():
#         msg = await p2.recv()
#         print(msg)

#         msg = await p1.recv()
#         print(msg)

#     asyncio.run(main())

    # msg = p2.recv_sync()
    # print(msg)

    # msg = p1.recv_sync()
    # print(msg)
