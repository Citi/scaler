
add_executable(echo_server echo_server.cpp)
add_executable(timestamp timestamp.cpp)

target_link_libraries(echo_server cc_ymq)
target_link_libraries(timestamp cc_ymq)
