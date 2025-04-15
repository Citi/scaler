
cmake_minimum_required(VERSION 3.28 FATAL_ERROR)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_CXX_SCAN_FOR_MODULES ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

project(
  scaler_object
  LANGUAGES C CXX
  VERSION 0.1
)

enable_testing()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
  message(STATUS "${PROJECT_NAME} Release build")
else()
  set(CMAKE_BUILD_TYPE "Debug")
  message(STATUS "${PROJECT_NAME} Debug build")
endif()


find_package(CapnProto CONFIG REQUIRED)
message(STATUS "Found CapnProto")

find_package(Boost CONFIG REQUIRED)
message(STATUS "Found Boost in ${Boost_INCLUDE_DIRS}")

include_directories(${Boost_INCLUDE_DIRS})


add_executable(server)
target_sources(server PRIVATE
  server.cpp
  io_helper.cpp
)

capnp_generate_cpp(messageSources messageHeaders object_instruction_header.capnp)

target_sources(server PRIVATE ${messageSources})
target_link_libraries(server PRIVATE CapnProto::capnp)
target_include_directories(server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})


add_subdirectory(tests)
