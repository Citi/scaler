add_library(cc_ymq SHARED)

target_sources(cc_ymq PRIVATE
  bytes.h
  common.h
  configuration.h
  epoll_context.h
  event_loop_backend.h
  event_loop.h
  event_loop_thread.cpp
  event_loop_thread.h
  event_manager.h
  file_descriptor.h 
  interruptive_concurrent_queue.h
  io_context.cpp
  io_context.h
  io_socket.cpp
  io_socket.h
  main.h
  message_connection.h
  message_connection_tcp.h
  tcp_client.h
  tcp_server.h
  third_party/concurrentqueue.h
  timed_concurrent_queue.h
)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/scaler/io/ymq)

install(TARGETS cc_ymq
        LIBRARY DESTINATION scaler/io/ymq)

find_package(Python COMPONENTS Development)

add_library(ymq SHARED)

set_target_properties(ymq PROPERTIES  PREFIX "")
set_target_properties(ymq PROPERTIES LINKER_LANGUAGE CXX)

target_sources(ymq PRIVATE pymod_ymq/bytes.h
                           pymod_ymq/message.h
                           pymod_ymq/io_context.h
                           pymod_ymq/io_socket.h
                           pymod_ymq/ymq.h
                           pymod_ymq/ymq.cpp
)
target_include_directories(ymq PRIVATE ${Python_INCLUDE_DIRS})
target_link_libraries(ymq PRIVATE cc_ymq
                          PRIVATE ${Python_LIBRARIES}
)

target_link_options(ymq PRIVATE "-Wl,-rpath,$ORIGIN,-L/home/george/.pyenv/versions/3.10.17/lib")

install(TARGETS ymq
       LIBRARY DESTINATION scaler/io/ymq)

add_subdirectory(examples)
