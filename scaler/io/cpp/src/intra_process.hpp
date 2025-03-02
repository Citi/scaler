#pragma once

// C
#include <cstddef>
#include <cstdint>

// C++
#include <optional>
#include <string>
#include <vector>
#include <shared_mutex>

// Third-party
#include "third_party/concurrentqueue.h"

// Common
#include "common.hpp"

using moodycamel::ConcurrentQueue;

// --- declarations ---
struct IntraProcessClient;

// First-party
#include "common.hpp"
#include "session.hpp"

void intraprocess_init(Session *session, IntraProcessClient *client, uint8_t *identity, size_t len);
void intraprocess_bind(struct IntraProcessClient *client, const char *addr, size_t len);
void intraprocess_connect(struct IntraProcessClient *client, const char *addr, size_t len);
void intraprocess_send(struct IntraProcessClient *client, uint8_t *data, size_t len);
void intraprocess_recv_sync(struct IntraProcessClient *client, struct Message *msg);
void intraprocess_recv_async(void *future, struct IntraProcessClient *client);
void intraprocess_destroy(struct IntraProcessClient *client);

// -- structs --

// inproc sockets are always pair sockets
struct IntraProcessClient
{
    Session *session;
    ThreadContext *thread;

    ConcurrentQueue<Message> queue;
    ConcurrentQueue<void *> recv;

    int recv_buffer_event_fd;
    int recv_event_fd;
    int unmuted_event_fd;
    
    Bytes identity;
    std::optional<std::string> bind;
    std::optional<std::string> connecting;
    std::optional<IntraProcessClient *> peer;

    bool epoll;

    void ensure_epoll();
    void remove_from_epoll();
};
