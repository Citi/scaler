from cffi import FFI

builder = FFI()

with open("src/defs.h") as f:
    defs = f.read()

builder.cdef(defs)

import os

builder.set_source(
    "cpp",
    '#include "../src/main.h"',
    source_extension=".cpp",
    sources=[f"../src/{name}" for name in os.listdir("../src") if name != "main.h"],
    extra_compile_args=["-std=gnu++23", "-Wall", "-Wextra", "-Werror"],
    language="c++",
)

if __name__ == "__main__":
    builder.compile(verbose=True, tmpdir="build")
