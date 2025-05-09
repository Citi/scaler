cmake_minimum_required(VERSION 4.0)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(example)

target_sources(example PRIVATE src/example.cpp)

# Add sanitizers in debug mode
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(example PRIVATE -fsanitize=address -fsanitize=leak -fsanitize=undefined)
    target_link_options(example PRIVATE -fsanitize=address -fsanitize=leak -fsanitize=undefined)
endif()
