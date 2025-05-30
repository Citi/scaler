#pragma once

extern "C" {
int create_server_ready_eventfd();

void wait_server_ready_eventfd(int on_server_ready_fd);

void set_server_ready_eventfd(int on_server_ready_fd);

void run_object_storage_server(const char* name, const char* port, int on_server_ready_fd);
}
