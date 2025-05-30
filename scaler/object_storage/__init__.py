import os
import sys

# Resolve paths relative to the current script location
current_dir = os.path.dirname(os.path.abspath(__file__))
lib_path = os.path.abspath(os.path.join(current_dir, "../lib"))
fallback_path = os.path.abspath(os.path.join(current_dir, "../../build/scaler/object_storage"))

# Check which directory exists and update PYTHONPATH accordingly
if os.path.isdir(lib_path):
    sys.path.append(lib_path)
else:
    sys.path.append(fallback_path)

# Now try to import the module
try:
    import object_storage_server
except ImportError as e:
    print("Failed to import object_storage_server:", e)
