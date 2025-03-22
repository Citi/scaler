from cffi import FFI

builder = FFI()

with open("src/defs.h") as f:
    defs = f.read()

builder.cdef(defs)

import os

builder.set_source(
    "cpp",
    '#include "../src/main.hpp"',
    source_extension=".cpp",
    # runs gcc like it's g++ and links the C++ standard library
    extra_compile_args=(
        ["-std=c++23", "-Wall", "-Wextra", "-pedantic"]
        if os.getenv("CC") == "clang"
        else ["-xc++", "-lstdc++", "-shared-libgcc", "-std=gnu++23", "-Wall", "-Wextra", "-Werror"]
    ),
    language="c++",
)

if __name__ == "__main__":
    builder.compile(verbose=True, tmpdir="build")

    import shutil, glob
    shutil.copy(glob.glob("build/cpp.*.so")[0], ".")
