import os
import shutil
import subprocess
import sys

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext


def is_building_wheel():
    return "bdist_wheel" in sys.argv


def is_building_sdist():
    return "sdist" in sys.argv


class CMakeBuild(build_ext):
    def run(self):
        try:
            subprocess.check_output(["cmake", "--version"])
        except OSError:
            raise RuntimeError("CMake must be installed to build the extensions.")

        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        if is_building_wheel():
            # Configure the CMake project
            subprocess.check_call(["cmake", "-S", ".", "-B", "build"])

            # Build the project
            subprocess.check_call(["cmake", "--build", "build"])

            # Copy the shared object to the Python package
            self.copy_so_file()

    def copy_so_file(self):
        # so_src = os.path.join('build', 'scaler', 'object_storage', 'libserver.so')
        so_src = os.path.join("build/scaler/object_storage/", "libserver.so")
        so_dst_dir = os.path.join(self.build_lib, "scaler/lib")
        os.makedirs(so_dst_dir, exist_ok=True)
        if os.path.exists(so_src):
            shutil.copy2(so_src, so_dst_dir)
        else:
            raise FileNotFoundError(f"Expected built file not found: {so_src}")


# class CMakeSdist(_sdist):
#     def run(self):
#         # Ensure CMake build is complete before packaging
#         subprocess.check_call(['cmake', '-S', '.', '-B', 'build'])
#         subprocess.check_call(['cmake', '--build', 'build'])
#
#         # Copy the .so file to scaler/lib/ in the source tree before sdist
#         so_src = os.path.join("build/scaler/object_storage/", "libserver.so")
#         so_dst = os.path.join("scaler/lib", 'libserver.so')
#         os.makedirs(os.path.dirname(so_dst), exist_ok=True)
#         if os.path.exists(so_src):
#             shutil.copy2(so_src, so_dst)
#         else:
#             raise FileNotFoundError(f"Expected built file not found: {so_src}")
#
#         super().run()


# Dummy extension to trigger build_ext
ext_modules = [Extension("cmake", sources=[])]

setup(
    ext_modules=ext_modules,
    cmdclass={
        "build_ext": CMakeBuild,
        # 'sdist': CMakeSdist,
    },
    zip_safe=False,
    package_data={"scaler": ["lib/libserver.so"]},
)
