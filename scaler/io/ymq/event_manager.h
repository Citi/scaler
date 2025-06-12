#pragma once

// C++
#include <cstdint>  // uint64_t
#include <functional>
#include <memory>

// First-party
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/file_descriptor.h"

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

    std::shared_ptr<EventLoopThread> _eventLoopThread;
    FileDescriptor _fd;
    Callback _callback;

    // must happen on io thread
    void removeFromEventLoop();

public:
    EventManager(std::shared_ptr<EventLoopThread> thread, FileDescriptor&& fd, Callback callback)
        : _eventLoopThread(thread), _fd(std::move(fd)), _callback(std::move(callback)) {}

    ~EventManager() { removeFromEventLoop(); }

    // must happen on io thread
    void addToEventLoop();

    bool operator==(const EventManager& other) const { return this->_fd == other._fd; }

    void onEvent(Events events) { this->_callback(_fd, events); }

    friend class EpollContext;
};
