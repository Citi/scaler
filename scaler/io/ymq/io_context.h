#pragma once

// C++
#include <memory>
#include <vector>

// First-party
#include "configuration.h"
#include "event_loop_thread.h"
#include "io_socket.h"

using Identity = configuration::Identity;

class IOSocket;

class IOContext {
    // This is a pointer, just for now
    std::vector<EventLoopThread> _threads;

public:
    IOContext()                            = default;
    IOContext(const IOContext&)            = delete;
    IOContext& operator=(const IOContext&) = delete;
    IOContext(IOContext&&)                 = delete;
    IOContext& operator=(IOContext&&)      = delete;

    // These methods need to be thread-safe.
    std::shared_ptr<IOSocket> createIOSocket(Identity identity, SocketTypes socketType);
    bool removeIOSocket(std::shared_ptr<IOSocket>);
};
