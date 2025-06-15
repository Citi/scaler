#pragma once

// C++
#include <memory>
#include <vector>

// First-party
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/typedefs.h"

// NOTE: Don't do this in a header file, it will pollute the env. - gxu
// using Identity = Configuration::Identity;

class IOSocket;
class EventLoopThread;

class IOContext {
    std::vector<std::shared_ptr<EventLoopThread>> _threads;

    using Identity = Configuration::Identity;

public:
    IOContext(size_t threadCount = 1);

    IOContext(const IOContext&)            = delete;
    IOContext& operator=(const IOContext&) = delete;
    IOContext(IOContext&&)                 = delete;
    IOContext& operator=(IOContext&&)      = delete;

    // These methods need to be thread-safe.
    std::shared_ptr<IOSocket> createIOSocket(Identity identity, IOSocketType socketType);

    bool removeIOSocket(std::shared_ptr<IOSocket>);

    size_t numThreads() const { return _threads.size(); }
};
