#!/bin/bash -e

if [[ "$1" != "Debug" && "$1" != "Release" ]]; then
    echo "Usage: ./scripts/build.sh [Debug|Release]"
    exit 1
fi

BUILD_TYPE="$1"
cmake -S . -B build -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -GNinja
cd build && ninja
cp scaler/io/ymq/libymq.so ../scaler/io/ymq/.
cp scaler/object_storage/*.so ../scaler/object_storage/.
