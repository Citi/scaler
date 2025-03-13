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
struct IntraProcessConnector;

// First-party
#include "common.hpp"
#include "session.hpp"

void intraprocess_init(Session *session, IntraProcessConnector *connector, uint8_t *identity, size_t len);
void intraprocess_bind(struct IntraProcessConnector *connector, const char *addr, size_t len);
void intraprocess_connect(struct IntraProcessConnector *connector, const char *addr, size_t len);
void intraprocess_send(struct IntraProcessConnector *connector, uint8_t *data, size_t len);
void intraprocess_recv_sync(struct IntraProcessConnector *connector, struct Message *msg);
void intraprocess_recv_async(void *future, struct IntraProcessConnector *connector);
void intraprocess_destroy(struct IntraProcessConnector *connector);

// -- structs --

// inproc sockets are always pair sockets
struct IntraProcessConnector
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
    std::optional<IntraProcessConnector *> peer;

    bool epoll;

    void ensure_epoll();
    void remove_from_epoll();
};
