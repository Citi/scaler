#pragma once

// C++
#include <sys/types.h>

#include <functional>

// First-party
#include "event_loop_thread.hpp"
#include "scaler/io/ymq/file_descriptor.hpp"

struct EventLoopThread;

class EventManager {
    using Events   = u_int64_t;
    using Callback = std::function<void(FileDescriptor&, Events)>;

    EventLoopThread& thread;
    FileDescriptor fd;
    Callback callback;

    void removeFromEventLoop();

public:
    EventManager(EventLoopThread& thread, FileDescriptor&& fd, Callback callback)
        : thread(thread), fd(std::move(fd)), callback(std::move(callback)) {}

    ~EventManager() { removeFromEventLoop(); }

    void addToEventLoop();

    bool operator==(const EventManager& other) const { return this->fd == other.fd; }

    void onEvent(Events events) { this->callback(fd, events); }

    friend class EpollContext;
};
