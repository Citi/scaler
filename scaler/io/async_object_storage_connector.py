import asyncio
import dataclasses
from typing import Awaitable, Callable, Optional, Tuple

from scaler.protocol.capnp._python import _object_storage  # noqa
from scaler.protocol.python.object_storage import ObjectRequestHeader, ObjectResponseHeader
from scaler.utility.exceptions import ObjectStorageException


@dataclasses.dataclass
class ObjectStorageGetResponse:
    object_id: bytes
    payload: bytes


class AsyncObjectStorageConnector:
    """An asyncio connector that uses an raw TCP socket to connect to a Scaler's object storage instance."""

    def __init__(self):
        self._host: Optional[str] = None
        self._port: Optional[int] = None

        self._connected_event = asyncio.Event()

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

        self._on_object_get_response: Optional[Callable[[ObjectStorageGetResponse], Awaitable[None]]] = (
            on_object_get_response
        )

    def __del__(self):
        if not self.is_connected():
            return

        self._writer.close()

    async def connect(self, host: str, port: int):
        self._host = host
        self._port = port

        if self.is_connected():
            raise ObjectStorageException("connector is already connected.")

        self._reader, self._writer = await asyncio.open_connection(self._host, self._port)

        self._connected_event.set()

    async def wait_until_connected(self):
        await self._connected_event.wait()

    def is_connected(self) -> bool:
        return self._connected_event.is_set()

    async def destroy(self):
        if not self.is_connected():
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
        self.__ensure_is_connected()
        return f"tcp://{self._host}:{self._port}"

    async def routine(self):
        await self.wait_until_connected()

        if self._on_object_get_response is None:
            return

        response = await self.__receive_response()
        if response is None:
            return

        header, payload = response

        if header.response_type == ObjectResponseHeader.ObjectResponseType.GetOK:
            await self._on_object_get_response(ObjectStorageGetResponse(header.object_id, payload))

<<<<<<< HEAD
    async def send_set_request(self, object_id: bytes, payload: bytes) -> None:
=======
        pending_get_future = self._pending_get_requests.get(header.object_id)

        if pending_get_future is None:
            logging.warning(f"unknown get-ok response for unrequested object_id={header.object_id.hex()}.")
            return

        pending_get_future.set_result(payload)

    async def set_object(self, object_id: bytes, payload: bytes) -> None:
>>>>>>> 2222e0c (fixup! Adds a asyncio connector for the Object Storage protocol.)
        await self.__send_request(object_id, len(payload), ObjectRequestHeader.ObjectRequestType.SetObject, payload)

    async def send_get_request(self, object_id: bytes, max_payload_length: int = 2**64 - 1) -> None:
        await self.__send_request(object_id, max_payload_length, ObjectRequestHeader.ObjectRequestType.GetObject, None)

    async def send_delete_request(self, object_id: bytes) -> None:
        await self.__send_request(object_id, 0, ObjectRequestHeader.ObjectRequestType.DeleteObject, None)

    def __ensure_is_connected(self):
        if self._writer is None:
            raise ObjectStorageException("connector is not connected.")

        if self._writer.is_closing():
            raise ObjectStorageException("connection is closed.")

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

        try:
            await self._writer.drain()
        except ConnectionResetError:
            self.__raise_connection_failure()

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

        try:
            header = await self.__read_response_header()
            payload = await self.__read_response_payload(header)
        except asyncio.IncompleteReadError:
            self.__raise_connection_failure()

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

    @staticmethod
    def __raise_connection_failure():
        raise ObjectStorageException("connection failure to object storage server.")
