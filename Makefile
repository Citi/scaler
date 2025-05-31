debug:
	cmake -S . -B build -DCMAKE_CXX_COMPILER=$(shell which clang++) -DCMAKE_BUILD_TYPE=Debug && cd build && make -j$(nproc)

release:
	cmake -S . -B build -DCMAKE_CXX_COMPILER=$(shell which clang++) -DCMAKE_BUILD_TYPE=Release && cd build && make -j$(nproc)
