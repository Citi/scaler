__ALL__ = ["FFITypes", "ffi", "lib", "c_async", "c_async_wrap"]

from .cpp import ffi, lib
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

    def connector_init(session: "FFITypes.CData", connector: "FFITypes.CData", transport: int, type_: int, identity: bytes, len: int) -> None:
        (session, connector, transport, type_, identity, len)

    def connector_bind(connector: "FFITypes.CData", host: bytes, port: int) -> None:
        (connector, host, port)

    def connector_connect(connector: "FFITypes.CData", host: bytes, port: int) -> None:
        (connector, host, port)

    def connector_send_async(
        future: "FFITypes.CData", connector: "FFITypes.CData", to: bytes, to_len: int, data: bytes, data_len: int
    ) -> None:
        (future, connector, to, to_len, data, data_len)

    def connector_send_sync(connector: "FFITypes.CData", to: bytes, to_len: int, data: bytes, data_len: int) -> None:
        (connector, to, to_len, data, data_len)

    def connector_recv_async(future: "FFITypes.CData", connector: "FFITypes.CData") -> None:
        (future, connector)

    def connector_recv_sync(connector: "FFITypes.CData", msg: "FFITypes.CData") -> None:
        (connector, msg)

    def connector_destroy(connector: "FFITypes.CData") -> None:
        (connector,)


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
