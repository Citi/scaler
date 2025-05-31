import os
import sys

# Resolve paths relative to the current script location
current_dir = os.path.dirname(os.path.abspath(__file__))
pymod_path = os.path.abspath(os.path.join(current_dir, '../../build/scaler/object_storage'))

sys.path.append(pymod_path)

# Now try to import the module
try:
    import object_storage_server
except ImportError as e:
    raise ImportError(f"Failed to import object_storage_server from {pymod_path}") from e
