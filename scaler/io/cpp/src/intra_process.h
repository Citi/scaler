#pragma once

// C++
#include <optional>
#include <string>
#include <vector>
#include <shared_mutex>

// Third-party
#include "third_party/concurrentqueue.h";

// First-party
#include "session.h"
#include "common.h"

using moodycamel::ConcurrentQueue;

// inproc sockets are always pair sockets
struct IntraProcessClient
{
    size_t id;
    Session *session;

    ConcurrentQueue<Message> queue;
    ConcurrentQueue<void *> recv;

    int recv_buffer_event_fd;
    int recv_event_fd;

    std::optional<std::string> connecting;

    Bytes identity;
    std::optional<std::string> bind;
    std::optional<size_t> peer;
};

// --- public api ---
