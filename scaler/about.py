try:
    with open("version.txt", "r") as f:
        __version__ = f.read().strip()
except FileNotFoundError as e:
    raise FileNotFoundError(f"Failed to find version.txt") from e
