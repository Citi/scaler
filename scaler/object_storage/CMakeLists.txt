add_library(server SHARED)

target_sources(server PRIVATE
  server.cpp
  io_helper.cpp
)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/scaler/object_storage)

set(CAPNPC_SRC_PREFIX "${PROJECT_SOURCE_DIR}/scaler/protocol/capnp" CACHE STRING "" FORCE)
set(CAPNPC_OUTPUT_DIR "${CMAKE_BINARY_DIR}/protocol" CACHE STRING "" FORCE)

file(MAKE_DIRECTORY "${CAPNPC_OUTPUT_DIR}")

capnp_generate_cpp(objectStorageSources objectStorageHeaders "../protocol/capnp/object_storage.capnp")

target_sources(server PRIVATE ${objectStorageSources})
target_link_libraries(server PRIVATE CapnProto::capnp)
target_include_directories(server PRIVATE ${CMAKE_BINARY_DIR})

install(TARGETS server
        LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/scaler/lib)

find_package(Python COMPONENTS Development)

add_library(object_storage_server SHARED)

set_target_properties(object_storage_server PROPERTIES  PREFIX "")

target_sources(object_storage_server PRIVATE pymod_object_storage_server.cpp)
target_include_directories(object_storage_server PRIVATE ${Python_INCLUDE_DIRS})
target_link_libraries(object_storage_server PRIVATE server
                                            PRIVATE ${Python_LIBRARIES}
)

install(TARGETS object_storage_server
        LIBRARY DESTINATION ${CMAKE_SOURCE_DIR}/scaler/lib)
