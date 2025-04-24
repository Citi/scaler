import asyncio
import dataclasses
from typing import Awaitable, Callable, Optional

from scaler.protocol.capnp._python import _object_storage  # noqa
from scaler.protocol.python.object_storage import ObjectRequestHeader, ObjectResponseHeader


@dataclasses.dataclass
class ObjectStorageRequest:
    object_id: bytes
    request_type: ObjectRequestHeader.ObjectRequestType
    payload: bytes


@dataclasses.dataclass
class ObjectStorageResponse:
    object_id: bytes
    response_type: ObjectResponseHeader.ObjectResponseType
    payload: bytes


class AsyncObjectStorageConnector:
    """An asyncio connector that uses an raw TCP socket to connect to a Scaler's object storage instance."""

    def __init__(
        self,
        host: str,
        port: int,
        callback: Optional[Callable[[ObjectStorageResponse], Awaitable[None]]],
    ):
        self._host = host
        self._port = port

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

        self._callback: Optional[Callable[[ObjectStorageResponse], Awaitable[None]]] = callback

    def __del__(self):
        if self._writer is None:
            return

        self._writer.close()

    async def connect(self):
        if self._writer is not None:
            raise ConnectionError("connector is already connected.")

        self._reader, self._writer = await asyncio.open_connection(self._host, self._port)

    async def destroy(self):
        if self._writer is None:
            return

        if not self._writer.is_closing:
            self._writer.close()

        await self._writer.wait_closed()

    @property
    def reader(self) -> Optional[asyncio.StreamReader]:
        return self._reader

    @property
    def writer(self) -> Optional[asyncio.StreamWriter]:
        return self._writer

    @property
    def address(self) -> str:
        return f"tcp://{self._host}:{self._port}"

    async def routine(self):
        self.__ensure_is_connected()

        if self._callback is None:
            return

        response: Optional[ObjectStorageResponse] = await self.receive()
        if response is None:
            return

        await self._callback(response)

    async def receive(self) -> Optional[ObjectStorageResponse]:
        assert self._reader is not None

        if self._writer.is_closing():
            return None

        header = await self.__read_response_header()
        payload = await self.__read_response_payload(header)

        return ObjectStorageResponse(header.object_id, header.response_type, payload)

    async def send(self, request: ObjectStorageRequest):
        self.__ensure_is_connected()
        assert self._writer is not None

        self.__write_request_header(request)
        self.__write_request_payload(request)

        await self._writer.drain()

    def __ensure_is_connected(self):
        if self._writer is None:
            raise ConnectionError("connector is not connected.")

        if self._writer.is_closing():
            raise ConnectionError("connector is closed.")

    async def __read_response_header(self) -> ObjectResponseHeader:
        assert self._reader is not None

        header_data = await self._reader.readexactly(ObjectResponseHeader.MESSAGE_LENGTH)

        with _object_storage.ObjectResponseHeader.from_bytes(header_data) as header_message:
            return ObjectResponseHeader(header_message)

    async def __read_response_payload(self, header: ObjectResponseHeader) -> bytes:
        assert self._reader is not None

        if header.payload_length > 0:
            return await self._reader.readexactly(header.payload_length)
        else:
            return b""

    def __write_request_header(self, request: ObjectStorageRequest):
        assert self._writer is not None

        header = ObjectRequestHeader.new_msg(request.object_id, len(request.payload), request.request_type)
        self._writer.write(header.get_message().to_bytes())

    def __write_request_payload(self, request: ObjectStorageRequest):
        assert self._writer is not None

        self._writer.write(request.payload)
