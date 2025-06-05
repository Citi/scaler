#pragma once

// C++
#include <functional>

// First-party
#include "event_loop_thread.h"
#include "file_descriptor.h"

struct EventLoopThread;

// an io-facility-agnostic representation of event types
struct Events {
    bool readable : 1;
    bool writable : 1;

    static Events fromEpollEvents(uint32_t epollEvents) {
        return Events {
            .readable = (epollEvents & EPOLLIN) > 0,
            .writable = (epollEvents & EPOLLOUT) > 0,
        };
    }
};

class EventManager {
    using Callback = std::function<void(FileDescriptor&, Events)>;

    EventLoopThread& thread;
    FileDescriptor fd;
    Callback callback;

    // must happen on io thread
    void removeFromEventLoop();

public:
    EventManager(EventLoopThread& thread, FileDescriptor&& fd, Callback callback)
        : thread(thread), fd(std::move(fd)), callback(std::move(callback)) {}

    ~EventManager() { removeFromEventLoop(); }

    // must happen on io thread
    void addToEventLoop();

    bool operator==(const EventManager& other) const { return this->fd == other.fd; }

    void onEvent(Events events) { this->callback(fd, events); }

    friend class EpollContext;
};
