set(BUILD_SHARED_LIBS ON CACHE BOOL "Build shared libraries")

include(FetchContent)
FetchContent_Declare(
    CapnProto
    GIT_REPOSITORY https://github.com/capnproto/capnproto.git
)
FetchContent_MakeAvailable(CapnProto)

# message(STATUS "Fetched argparse from https://github.com/p-ranav/argparse.git")
message(STATUS "Fetched capnp from https://github.com/capnproto/capnproto.git")
# find_package(CapnProto CONFIG REQUIRED)
# message(STATUS "Found CapnProto")

add_subdirectory(object_storage)

