"""
This example uses the Pillow library with Scaler to resize images in parallel.
"""

import os
import sys
from multiprocessing import cpu_count
from PIL import Image, UnidentifiedImageError
from scaler import SchedulerClusterCombo, Client


def process_image(path: str):
    try:
        im = Image.open(path)
    except UnidentifiedImageError:
        return  # ignore non-image files

    # resize the image if it's too big
    if im.width > 1024 or im.height > 1024:
        im.thumbnail((1024, 1024))

    # this works because the workers are being run on the same machine as the client
    im.save(path)
    im.close()


def main():
    script_path = os.path.dirname(os.path.abspath(__file__))

    if len(sys.argv) != 2:
        dir = os.path.join(script_path, "images")
        print(f"Directory not provided as argument, using default: {dir}")
    else:
        dir = sys.argv[1]

    cluster = SchedulerClusterCombo(n_workers=cpu_count())
    client = Client(address=cluster.get_address())

    client.map(process_image, [(os.path.join(dir, f),) for f in os.listdir(dir)])


if __name__ == "__main__":
    main()
