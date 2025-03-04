__ALL__ = ["FFITypes", "ffi", "lib", "c_async", "c_async_wrap"]

import sys
from os import path

sys.path.append(path.join(path.dirname(__file__), "build"))
from cpp import ffi, lib

sys.path.pop()

from cffi import FFI as FFITypes


class LibType:
    Pair: int
    Pub: int
    Sub: int
    Dealer: int
    Router: int

    TCP: int
    IntraProcess: int
    InterProcess: int

    def session_init(session: "FFITypes.CData", num_threads: int) -> None:
        (session, num_threads)

    def session_destroy(session: "FFITypes.CData") -> None:
        (session,)

    def message_destroy(recv: "FFITypes.CData") -> None:
        (recv,)

    def client_init(session: "FFITypes.CData", client: "FFITypes.CData", identity: bytes, len: int, type_: int) -> None:
        (session, client, identity, len, type_)

    def client_bind(client: "FFITypes.CData", host: bytes, port: int) -> None:
        (client, host, port)

    def client_connect(client: "FFITypes.CData", addr: bytes, port: int) -> None:
        (client, addr, port)

    def client_send(
        future: "FFITypes.CData", client: "FFITypes.CData", to: bytes, to_len: int, data: bytes, data_len: int
    ) -> None:
        (future, client, to, to_len, data, data_len)

    def client_recv(future: "FFITypes.CData", client: "FFITypes.CData") -> None:
        (future, client)

    def client_destroy(client: "FFITypes.CData") -> None:
        (client,)

    def client_send_sync(client: "FFITypes.CData", to: bytes, to_len: int, data: bytes, data_len: int) -> None:
        (client, to, to_len, data, data_len)

    def client_recv_sync(client: "FFITypes.CData", msg: "FFITypes.CData") -> None:
        (client, msg)

    def intraprocess_init(session: "FFITypes.CData", inproc: "FFITypes.CData", identity: bytes, len: int) -> None:
        (session, inproc)

    def intraprocess_recv_async(future: "FFITypes.CData", inproc: "FFITypes.CData") -> None:
        (future, inproc)

    def intraprocess_recv_sync(inproc: "FFITypes.CData", msg: "FFITypes.CData") -> None:
        (inproc, msg)

    def intraprocess_send(inproc: "FFITypes.CData", data: bytes, len: int) -> None:
        (inproc, data, len)

    def intraprocess_connect(inproc: "FFITypes.CData", addr: bytes, len: int) -> None:
        (inproc, addr, len)

    def intraprocess_bind(inproc: "FFITypes.CData", addr: bytes, len: int) -> None:
        (inproc, addr, len)

    def intraprocess_destroy(client: "FFITypes.CData") -> None:
        (client,)


# type hints for FFI and Lib
ffi: FFITypes
lib: LibType

import asyncio
from typing import Callable, ParamSpec, TypeVar, Concatenate


class Message:
    __match_args__ = ("address", "payload")

    address: bytes
    payload: bytes

    def __init__(self, obj: "FFITypes.CData"):  # Message *
        # copy the address
        self.address = bytes(ffi.buffer(obj.address.data, obj.address.len))
        # copy the payload
        self.payload = bytes(ffi.buffer(obj.payload.data, obj.payload.len))


# this is called from C to inform the asyncio runtime that a future was completed
@ffi.def_extern()
def future_set_result(future_handle: "FFITypes.CData", result: "FFITypes.CData") -> None:
    if result == ffi.NULL:
        result = None
    else:
        msg = ffi.cast("struct Message *", result)
        result = Message(msg)

    future: asyncio.Future = ffi.from_handle(future_handle)

    if future.done():
        return

    # using `call_soon_threadsafe()` is very important:
    # - https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.call_soon_threadsafe
    future.get_loop().call_soon_threadsafe(future.set_result, result)


# these type variables make type hints work
# in Python 3.12+ we can use the new syntax instead of defining these:
# async def c_async[**P, R](...): ...
P = ParamSpec("P")
R = TypeVar("R")


# c_async is a helper function to call async C functions
# example: c_async(lib.async_binder_recv, binder)
async def c_async(fn: Callable[Concatenate["FFITypes.CData", P], R], *args: P.args, **kwargs: P.kwargs) -> R:
    future = asyncio.get_running_loop().create_future()
    handle = ffi.new_handle(future)
    fn(handle, *args, **kwargs)
    return await future
