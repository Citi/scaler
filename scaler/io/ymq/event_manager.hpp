#pragma once

// C++
#include <functional>

// First-party
#include "epoll_context.hpp"
#include "event_loop_thread.hpp"

class EventManager {
    using Events   = uint64_t;
    using Callback = std::function<void(FileDescriptor&, Events)>;

    EventLoopThread& thread;
    FileDescriptor fd;
    Callback callback;

public:
    EventManager(EventLoopThread& thread, FileDescriptor&& fd, Callback callback)
        : thread(thread), fd(std::move(fd)), callback(std::move(callback)) {
            thread.getEventLoop().registerEventManager(this);
        }

    ~EventManager() {
        thread.getEventLoop().removeEventManager(this);
        fd.~FileDescriptor();  // Close the file descriptor
    }

    bool operator==(const EventManager& other) const {
        return this->fd == other.fd;
    }

    void onEvent(Events events) {
        this->callback(fd, events);
    }

    friend class EpollContext;
};
