#pragma once

// C++
#include <mutex>
#include <string>
#include <vector>

// First-party
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/typedefs.h"

class IOSocket;

class IOContext {
    // This is a pointer, just for now
    std::vector<EventLoopThread> _threads;
    std::mutex _threadsMu;

public:
    IOContext(size_t threadCount): _threads(threadCount) {}
    IOContext(): _threads(1z) {}
    IOContext(const IOContext&)            = delete;
    IOContext& operator=(const IOContext&) = delete;
    IOContext(IOContext&&)                 = delete;
    IOContext& operator=(IOContext&&)      = delete;

    // These methods need to be thread-safe.
    IOSocket* addIOSocket(std::string identity, IOSocketType socketType);
    // ioSocket.getEventLoop().removeIOSocket(&ioSocket);
    bool removeIOSocket(IOSocket*);
};
