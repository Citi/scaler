#pragma once

// C
#include <cstddef>
#include <cstdint>

// C++
#include <atomic>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

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

Status intra_process_init(Session* session, IntraProcessConnector* connector, uint8_t* identity, size_t len);
Status intra_process_bind(struct IntraProcessConnector* connector, const char* addr);
Status intra_process_connect(struct IntraProcessConnector* connector, const char* addr);
Status intra_process_send(struct IntraProcessConnector* connector, uint8_t* data, size_t len);
Status intra_process_recv_sync(struct IntraProcessConnector* connector, struct Message* msg);
void intra_process_recv_async(void* future, struct IntraProcessConnector* connector);
Status intra_process_destroy(struct IntraProcessConnector* connector);

// -- structs --

// inproc sockets are always pair sockets
struct IntraProcessConnector {
    Session* session;
    ThreadContext* thread;

    ConcurrentQueue<Message> queue;
    ConcurrentQueue<void*> recv;

    int recv_buffer_event_fd;
    int recv_event_fd;
    int unmuted_event_fd;

    Bytes identity;
    std::optional<std::string> bind;
    std::optional<std::string> connecting;
    std::optional<IntraProcessConnector*> peer;

    std::atomic_bool epoll;

    Status ensure_epoll();
    void remove_from_epoll();
};
