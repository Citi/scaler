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
    Dealer: int
    Router: int

    def session_init(session: "FFITypes.CData", num_threads: int) -> None:
        (session, num_threads)

    def session_destroy(session: "FFITypes.CData") -> None:
        (session,)

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

    def message_destroy(recv: "FFITypes.CData") -> None:
        (recv,)

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


# type hints for FFI and Lib
ffi: FFITypes
lib: LibType

import asyncio
from typing import Callable, ParamSpec, TypeVar, Concatenate, Coroutine

# these type variables make type hints work
# in Python 3.12+ we can use the new syntax instead of defining these:
# async def c_async[**P, R](...): ...
P = ParamSpec("P")
R = TypeVar("R")

__future_keep_alive = []

# c_async is a helper function to call async C functions
# example: c_async(lib.async_binder_recv, binder)
async def c_async(fn: Callable[Concatenate["FFITypes.CData", P], R], *args: P.args, **kwargs: P.kwargs) -> R:
    future = asyncio.get_running_loop().create_future()
    handle = ffi.new_handle(future)
    __future_keep_alive.append(handle)
    fn(handle, *args, **kwargs)
    try:
        res = await future
    except BaseException as be:
        print("yoooo,", be)
        raise
    __future_keep_alive.remove(handle)
    return res


P2 = ParamSpec("P")
R2 = TypeVar("R")

# c_async_wrapper works like c_async but it returns a function
# that can be called rather than calling the function directly
# example: wrapped = c_async_wrap(lib.async_binder_recv, binder)
def c_async_wrapper(fn: Callable[Concatenate["FFITypes.CData", P2], R2]) -> Callable[P2, Coroutine[None, None, R2]]:
    async def inner(*args: P2.args, **kwargs: P2.kwargs) -> R2:
        future = asyncio.get_running_loop().create_future()
        handle = ffi.new_handle(future)
        fn(handle, *args, **kwargs)
        return await future

    return inner
