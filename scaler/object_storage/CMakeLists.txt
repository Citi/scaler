
find_package(Boost CONFIG REQUIRED)
message(STATUS "Found Boost in ${Boost_INCLUDE_DIRS}")

include_directories(${Boost_INCLUDE_DIRS})

add_executable(server)
target_sources(server PRIVATE
  server.cpp
  io_helper.cpp
)

set(CAPNPC_SRC_PREFIX "${PROJECT_SOURCE_DIR}/scaler/protocol/capnp" CACHE STRING "" FORCE)

set(CAPNPC_OUTPUT_DIR "${CMAKE_BINARY_DIR}/protocol" CACHE STRING "" FORCE)

file(MAKE_DIRECTORY "${CAPNPC_OUTPUT_DIR}")

capnp_generate_cpp(objectInstructionHeaderSources objectInstructionHeaderHeaders "../protocol/capnp/object_instruction_header.capnp")

target_sources(server PRIVATE ${objectInstructionHeaderSources})
target_link_libraries(server PRIVATE CapnProto::capnp)
target_include_directories(server PRIVATE ${CMAKE_BINARY_DIR})

add_subdirectory(tests)
