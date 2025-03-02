import logging
import threading
from typing import Callable, Optional


from scaler.io.utility import deserialize
from scaler.protocol.python.mixins import Message

from scaler.io.model import Client, Address, Session, ConnectorType

class SyncSubscriber(threading.Thread):
    def __init__(
        self,
        address: Address,
        callback: Callable[[Message], None],
        topic: bytes,
        exit_callback: Optional[Callable[[], None]] = None,
        stop_event: threading.Event = threading.Event(),
        daemonic: bool = False,
        timeout_seconds: int = -1,
    ):
        raise NotImplementedError

        threading.Thread.__init__(self)

        self._stop_event = stop_event
        self._address = address
        self._callback = callback
        self._exit_callback = exit_callback
        self._topic = topic
        self.daemon = bool(daemonic)
        self._timeout_seconds = timeout_seconds

        self._session: Session | None = None
        self._client: Client | None = None

    def __close(self):
        self._client.destroy()

    def __stop_polling(self):
        self._stop_event.set()

    def disconnect(self):
        self.__stop_polling()

    def run(self) -> None:
        self.__initialize()

        while not self._stop_event.is_set():
            self.__routine_polling()

        if self._exit_callback is not None:
            self._exit_callback()

        self.__close()

    def __initialize(self):
        self._session = Session(io_threads=1)
        self._client = Client(self._session, "sync_subscriber".encode(), ConnectorType.Sub)
        # self._context = zmq.Context.instance()
        # self._socket = self._context.socket(zmq.SUB)
        # self._socket.setsockopt(zmq.RCVHWM, 0)

        # if self._timeout_seconds == -1:
        #     self._socket.setsockopt(zmq.RCVTIMEO, self._timeout_seconds)
        # else:
        #     self._socket.setsockopt(zmq.RCVTIMEO, self._timeout_seconds * 1000)

        # self._socket.subscribe(self._topic)
        # self._socket.connect(self._address)
        # self._socket.connect(self._address)

    def __routine_polling(self):
        try:
            self.__routine_receive(self._socket.recv(copy=False).bytes)
        except zmq.Again:
            raise TimeoutError(f"Cannot connect to {self._address} in {self._timeout_seconds} seconds")

    def __routine_receive(self, payload: bytes):
        result: Optional[Message] = deserialize(payload)
        if result is None:
            logging.error(f"received unknown message: {payload!r}")
            return None

        self._callback(result)
