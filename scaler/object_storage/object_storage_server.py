import ctypes
import os

from scaler.utility.object_storage_config import ObjectStorageConfig


DEFAULT_OBJECT_STORAGE_LIB_PATH = os.path.join(os.path.dirname(__file__), "..", "lib", "libserver.so")


class ObjectStorageServer:
    def __init__(self, config: ObjectStorageConfig, lib_path: str = DEFAULT_OBJECT_STORAGE_LIB_PATH):
        self._config = config

        self._lib_path = lib_path

        self._on_server_ready_fd = os.eventfd(0, os.EFD_SEMAPHORE)
        os.set_inheritable(self._on_server_ready_fd, True)  # allows the FD to be passed to a spawned process

    def run(self) -> None:
        """Runs the object storage server, does not return."""

        lib = self._load_library(lib_path=self._lib_path)

        # Define the argument and return types for the main_entrance function
        lib.run_object_storage_server.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
        lib.run_object_storage_server.restype = None

        # Call the function
        name_bytes = self._config.host.encode("utf-8")
        port_bytes = str(self._config.port).encode("utf-8")

        lib.run_object_storage_server(name_bytes, port_bytes, self._on_server_ready_fd)

    def wait_until_ready(self) -> None:
        """Blocks until the object storage server is available to server requests."""
        os.eventfd_read(self._on_server_ready_fd)

    @staticmethod
    def _load_library(lib_path):
        try:
            return ctypes.CDLL(lib_path)
        except OSError as e:
            raise RuntimeError(f"Failed to load library: {e}")
