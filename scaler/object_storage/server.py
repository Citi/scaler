import argparse
import ctypes
import sys

def load_library(lib_path):
    """
    Load the shared dynamic library.
    """
    try:
        return ctypes.CDLL(lib_path)
    except OSError as e:
        print(f"Failed to load library: {e}")
        sys.exit(1)

def run_object_storage_server(lib_path: str, name: str = "127.0.0.1", port: str = "55555"):
    # Load the library
    lib = load_library(lib_path=lib_path)
    # Define the argument and return types for the main_entrance function
    lib.run_object_storage_server.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
    lib.run_object_storage_server.restype = None

    # Call the function
    name_bytes = name.encode('utf-8')
    port_bytes = port.encode('utf-8')

    lib.run_object_storage_server(name_bytes, port_bytes)

def main():
    parser = argparse.ArgumentParser(description='Call main_entrance from a dynamic library.')
    parser.add_argument('--lib', required=True, help='Path to the foundational library')
    parser.add_argument('--name', '-n', default='127.0.0.1', 
                        help='Ip address or NS record. Default to 127.0.0.1')
    parser.add_argument('--port', '-p', default='55555', 
                        help='Port number to connect. Default to 55555')
    args = parser.parse_args()

    run_object_storage_server(args.lib, args.name, args.port)


if __name__ == '__main__':
    main()

