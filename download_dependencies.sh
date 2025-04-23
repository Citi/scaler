#!/bin/bash

if [ "$1" == "boost" ]; then
	curl -L https://archives.boost.io/release/1.88.0/source/boost_1_88_0.tar.gz -o boost188.tar.gz
	tar -xzf boost188.tar.gz
	sudo mkdir -p /usr/include/boost
	sudo mv boost_1_88_0/boost /usr/include/.
	echo "Installed Boost into /usr/include/boost"

elif [ "$1" == "capnp" ]; then
	curl -O https://capnproto.org/capnproto-c++-1.1.0.tar.gz
	tar zxf capnproto-c++-1.1.0.tar.gz
	cd capnproto-c++-1.1.0
	./configure --prefix=/usr/
	make -j6 check
	sudo make install
	echo "Installed capnp into /usr"

else 
    echo "Usage: ./download_dependencies.sh [boost|capnp]"
    exit 1
fi

