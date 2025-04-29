import ctypes
import os.path
import sys

from scaler.utility.object_storage_config import ObjectStorageConfig


DEFAULT_OBJECT_STORAGE_LIB_PATH = os.path.join(os.path.dirname(__file__), "..", "lib", "libserver.so")


def load_library(lib_path):
    """
    Load the shared dynamic library.
    """
    try:
        return ctypes.CDLL(lib_path)
    except OSError as e:
        print(f"Failed to load library: {e}")
        sys.exit(1)


def run_object_storage_server(
    config: ObjectStorageConfig,
    lib_path: str = DEFAULT_OBJECT_STORAGE_LIB_PATH,
):
    # Load the library
    lib = load_library(lib_path=lib_path)
    # Define the argument and return types for the main_entrance function
    lib.run_object_storage_server.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
    lib.run_object_storage_server.restype = None

    # Call the function
    name_bytes = config.host.encode("utf-8")
    port_bytes = str(config.port).encode("utf-8")

    lib.run_object_storage_server(name_bytes, port_bytes)
