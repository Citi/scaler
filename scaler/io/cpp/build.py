from cffi import FFI

builder = FFI()

with open("src/defs.h") as f:
    defs = f.read()

builder.cdef(defs)

builder.set_source(
    "cpp",
    '#include "../src/main.h"',
    source_extension=".cpp",
    sources=["../src/main.cpp"],
    extra_compile_args=["-std=gnu++23", "-Wall", "-Wextra", "-Werror"],
    language="c++",
)

if __name__ == "__main__":
    builder.compile(verbose=True, tmpdir="build")
