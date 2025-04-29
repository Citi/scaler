import asyncio
from typing import Awaitable, Callable, Optional, Tuple

from scaler.protocol.capnp._python import _object_storage  # noqa
from scaler.protocol.python.object_storage import ObjectRequestHeader, ObjectResponseHeader


class AsyncObjectStorageConnector:
    """An asyncio connector that uses an raw TCP socket to connect to a Scaler's object storage instance."""

    def __init__(
        self,
        host: str,
        port: int,
        on_object_get_response: Optional[Callable[[bytes, bytes], Awaitable[None]]],  # TODO: add comment on argument types
    ):
        self._host = host
        self._port = port

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

        self._on_object_get_response: Optional[Callable[[bytes, bytes], Awaitable[None]]] = on_object_get_response

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

        if self._on_object_get_response is None:
            return

        response = await self.__receive_response()
        if response is None:
            return

        header, payload = response

        if header.response_type == ObjectResponseHeader.ObjectResponseType.GetOK:
            await self._on_object_get_response(header.object_id, payload)

    async def send_set_request(self, object_id: bytes, payload: bytes) -> None:
        await self.__send_request(object_id, len(payload), ObjectRequestHeader.ObjectRequestType.SetObject, payload)

    async def send_get_request(self, object_id: bytes, max_payload_length: int = 2**64 - 1) -> None:
        await self.__send_request(object_id, max_payload_length, ObjectRequestHeader.ObjectRequestType.GetObject, None)

    async def send_delete_request(self, object_id: bytes) -> None:
        await self.__send_request(object_id, 0, ObjectRequestHeader.ObjectRequestType.DeleteObject, None)

    def __ensure_is_connected(self):
        if self._writer is None:
            raise ConnectionError("connector is not connected.")

        if self._writer.is_closing():
            raise ConnectionError("connector is closed.")

    async def __send_request(
        self,
        object_id: bytes,
        payload_length: int,
        request_type: ObjectRequestHeader.ObjectRequestType,
        payload: Optional[bytes],
    ):
        self.__ensure_is_connected()
        assert self._writer is not None

        header = ObjectRequestHeader.new_msg(object_id, payload_length, request_type)

        self.__write_request_header(header)

        if payload is not None:
            self.__write_request_payload(payload)

        await self._writer.drain()

    def __write_request_header(self, header: ObjectRequestHeader):
        assert self._writer is not None
        self._writer.write(header.get_message().to_bytes())

    def __write_request_payload(self, payload: bytes):
        assert self._writer is not None
        self._writer.write(payload)

    async def __receive_response(self) -> Optional[Tuple[ObjectResponseHeader, bytes]]:
        assert self._reader is not None

        if self._writer.is_closing():
            return None

        header = await self.__read_response_header()
        payload = await self.__read_response_payload(header)

        return header, payload

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
