#!/bin/bash -e

cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cd build && make
cp scaler/io/ymq/libymq.so ../scaler/io/ymq/.
cp scaler/object_storage/*.so ../scaler/object_storage/.
ctest
