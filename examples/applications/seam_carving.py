"""
This example implements the popular seam carving algorithm[0] for content-aware image resizing in parallel[1] using Scaler.

This program draws the minimum energy seam on the image, which is a path of pixels that minimizes the total energy of the image.
The energy of a pixel is calculated using the euclidean distance with all 8 neighbors.
Removing the seam will reduce the image size by one pixel in the cross direction and is left as an exercise for the reader.

As per Sam Westrick's blog post, the image is divided into strips with a height half that of the triangle width (even number) plus one.
This strip is then tessellated into triangles where the triangles pointing down are processed first in parallel, followed by the triangles pointing up, also in parallel.

---

[0]: https://en.wikipedia.org/wiki/Seam_carving
[1]: https://shwestrick.github.io/2020/07/29/seam-carve.html
"""

import sys
import itertools
import numpy as np
from math import ceil
from timeit import default_timer as timer
from PIL import Image, UnidentifiedImageError
from scaler import SchedulerClusterCombo, Client

TRIANGLE_BASE_WIDTH: int = 120

# needs to be an even number
assert TRIANGLE_BASE_WIDTH % 2 == 0, "TRIANGLE_BASE_WIDTH must be an even number"


def calc_down_triangle(x: int, y: int, im: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    # stores the energies for each pixel in the triangle
    energies = np.zeros((TRIANGLE_BASE_WIDTH, TRIANGLE_BASE_WIDTH // 2 + 1), dtype=np.float64)

    # these are our bounds in `im`
    # we shift right by 1 because of padding
    left = x
    right = left + TRIANGLE_BASE_WIDTH
    top = y
    bottom = top + TRIANGLE_BASE_WIDTH // 2 + 1

    # calculate the energies for each pixel in the triangle using euclidean distance with all 8 neighbors
    for (a, b) in itertools.product((-1, 0, 1), repeat=2):
        # shift the area by (a, b), subtract, and norm to calculate euclidean distance with neighbors
        energies += np.apply_along_axis(np.linalg.norm, 2, im[left : right, top : bottom] - im[left + a : right + a, top + b : bottom + b])

    # our dynamic programming table
    # each (x, y) stores the minimum energy to get to that pixel from the top
    # thus (x, bottom) is the the minimum energy seam to the xth pixel in the bottom row
    # we will then be able to recover the seam by tracing upwards
    dp = np.zeros((TRIANGLE_BASE_WIDTH, TRIANGLE_BASE_WIDTH // 2 + 1), dtype=np.float64)

    # initialize the first row
    dp[:, 0] = energies[:, 0]

    for row in range(1, TRIANGLE_BASE_WIDTH // 2):
        for col in range(TRIANGLE_BASE_WIDTH):
            # skip pixels that are outside the triangle
            if col - row < 0 or col + row >= TRIANGLE_BASE_WIDTH:
                continue

            dp[col, row] = energies[col, row] + min(
                dp[col, row - 1],  # directly above
                dp[col - 1, row - 1] if col > 0 else np.inf,  # top left
                dp[col + 1, row - 1] if col < TRIANGLE_BASE_WIDTH - 1 else np.inf,  # top right
            )

    return energies, dp

def calc_up_triangle(x: int, y: int, energies: np.ndarray, dp: np.ndarray) -> np.ndarray:
    left = x
    right = left + TRIANGLE_BASE_WIDTH
    top = 0 # dp is a strip
    bottom = top + TRIANGLE_BASE_WIDTH // 2 + 1

    dyn = dp[left : right, top : bottom]

    for row in range(TRIANGLE_BASE_WIDTH // 2):
        for col in range(TRIANGLE_BASE_WIDTH):
            if col < TRIANGLE_BASE_WIDTH // 2 - row - 1 or col > TRIANGLE_BASE_WIDTH // 2 + row:
                continue

            dyn[col, row + 1] = energies[col, row] + min(
                dyn[col, row],  # directly above
                dyn[col - 1, row] if col > 0 else np.inf,  # top left
                dyn[col + 1, row] if col < TRIANGLE_BASE_WIDTH - 1 else np.inf,  # top right
            )

    return dyn


def find_min_seam(dp: np.ndarray) -> list[int]:
    # min of last row
    i = np.argmin(dp[:, -1])

    seam = [i]

    for row in range(dp.shape[1] - 2, -1, -1):
        # we take the min of the three pixels above to recover the seam
        # m = np.argmin(dp[max(i - 1, 0) : min(i + 1, dp.shape[0] - 1), row])

        if i == 0:
            m = np.argmin(dp[i : i + 1, row])
        elif i == dp.shape[0] - 1:
            m = np.argmin(dp[i - 1 : i, row])
        else:
            m = np.argmin(dp[i - 1 : i + 1, row])

        i += m - 1

        if i < 0:
            i = 0
        elif i >= dp.shape[0]:
            i = dp.shape[0] - 1

        seam.append(i)

    # reversing creates a top-down order
    seam.reverse()

    return seam

def sequential_seam_carve(im: Image.Image, result: str):
    imdata = np.array(im)

    # rotate the image 90 degrees so that it's row-major
    # it doesn't really matter if it's cc or ccw
    imdata = imdata.transpose(1, 0, 2) # (width, height, [R, G, B])

    width = imdata.shape[0]
    height = imdata.shape[1]

    now = timer()

    imdata = np.pad(imdata, ((1, 1), (1, 1), (0, 0)), constant_values=0)
    energies = np.zeros((width, height), dtype=np.float64)

    # calculate the energies for each pixel using euclidean distance with all 8 neighbors
    for i, (a, b) in enumerate(itertools.product((-1, 0, 1), repeat=2)):
        print(f"Sequential: energies: {i / 8 * 100:.2f}%")

        # shift the area by (a, b), subtract, and norm to calculate euclidean distance with neighbors
        energies += np.apply_along_axis(
            np.linalg.norm, 2, imdata[1 : -1, 1 : -1] - imdata[1 + a : width + a + 1, 1 + b : height + b + 1])

    # our dynamic programming table
    dp = np.zeros((width, height), dtype=np.float64)
    dp[:, 0] = energies[:, 0]

    counter = 0

    for row in range(1, height):
        if counter % 25 == 0:
            print(f"Sequential: dp: {row / height * 100}%")

        counter += 1

        for col in range(width):
            dp[col, row] = energies[col, row] + min(
                dp[col, row - 1],  # directly above
                dp[col - 1, row - 1] if col > 0 else np.inf,  # top left
                dp[col + 1, row - 1] if col < width - 1 else np.inf,  # top right
            )

    # normalize dp
    dp[:] = (dp - np.min(dp)) / (np.max(dp) - np.min(dp))

    elapsed = timer() - now
    print(f"Sequential: elapsed time: {elapsed:.2f} seconds")

    np.savetxt("/tmp/dp.txt", dp)

    seam = find_min_seam(dp)

    for i, x in enumerate(seam):
        im.putpixel((x, i), (255, 0, 0))

    im.save(result)

def parallel_seam_carve(im: Image.Image, result: str):
    address = "tcp://127.0.0.1:2345"
    cluster = SchedulerClusterCombo(address=address, n_workers=6)
    client = Client(address=address)

    # warm up the cluster
    client.submit(lambda _: ..., None)

    imdata = np.array(im)

    # rotate the image 90 degrees so that it's row-major
    # it doesn't really matter if it's cc or ccw
    imdata = imdata.transpose(1, 0, 2) # (width, height, [R, G, B])

    width = imdata.shape[0]
    height = imdata.shape[1]

    # we need to pad imdata's width to a multiple of TRIANGLE_BASE_WIDTH
    new_width = ceil(width / TRIANGLE_BASE_WIDTH) * TRIANGLE_BASE_WIDTH

    # similarly, the algorithm is simpler if the height is a multiple of TRIANGLE_BASE_WIDTH // 2
    new_height = ceil(height / (TRIANGLE_BASE_WIDTH // 2 + 1)) * (TRIANGLE_BASE_WIDTH // 2 + 1)

    # pad the edges with 0s so that we can shift and not worry about going out of bounds
    imdata = np.pad(imdata, ((1, new_width + 1 - width), (1, new_height + 1 - height), (0, 0)), constant_values=0)

    dp = None

    now = timer()

    for y in range(0, new_height, TRIANGLE_BASE_WIDTH // 2 + 1):
        energies, dp_tmp = None, None

        results = client.map(calc_down_triangle, [(x, y + 1, imdata) for x in range(1, new_width + 1, TRIANGLE_BASE_WIDTH)])

        en = np.concatenate([tup[0] for tup in results], axis=0)
        dyn = np.concatenate([tup[1] for tup in results], axis=0)

        if energies is None:
            energies = en
        else:
            energies = np.concat((energies, en), axis=0)

        if dp_tmp is None:
            dp_tmp = dyn
        else:
            dp_tmp = np.concat((dp_tmp, dyn), axis=0)

        # pad each side by half of the base width so that we can have aligned upward triangles
        dp_tmp = np.pad(dp_tmp, ((TRIANGLE_BASE_WIDTH // 2, TRIANGLE_BASE_WIDTH // 2), (0, 0)))

        for x in range(0, dp_tmp.shape[0], TRIANGLE_BASE_WIDTH):
            dp_tmp[x : x + TRIANGLE_BASE_WIDTH, 0 : TRIANGLE_BASE_WIDTH // 2 + 1] = calc_up_triangle(x, y, energies, dp_tmp)

        # cut off padding
        dp_tmp = dp_tmp[TRIANGLE_BASE_WIDTH // 2 : -TRIANGLE_BASE_WIDTH // 2, :]
        dp_tmp = dp_tmp[: -(new_width - width), :]

        if dp is None:
            dp = dp_tmp
        else:
            dp = np.concat((dp, dp_tmp), axis=1)

    # cut off vertical padding
    dp = dp[:, new_height - height :]

    # normalize dp
    dp[:] = (dp - np.min(dp)) / (np.max(dp) - np.min(dp))

    elapsed = timer() - now
    print(f"Parallel: elapsed time: {elapsed:.2f} seconds")

    seam = find_min_seam(dp)

    # the image is not transposed so we flip the coordinates
    for i, x in enumerate(seam):
        im.putpixel((x, i), (255, 0, 0))

    im.save(result)

    cluster.shutdown()

def main():
    source, result_seq, result_par = sys.argv[1:]

    try:
        im = Image.open(source)
    except UnidentifiedImageError:
        print(f"Error: [{source}] is not a valid image file.")
        return
    
    sequential_seam_carve(im, result_seq)
    parallel_seam_carve(im, result_par)

if __name__ == "__main__":
    main()
