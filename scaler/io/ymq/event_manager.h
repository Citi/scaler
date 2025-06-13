#pragma once

// C++
#include <sys/epoll.h>

#include <concepts>
#include <cstdint>  // uint64_t
#include <functional>
#include <memory>

// First-party
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/file_descriptor.h"

class EventLoopThread;

class EventManager {
    std::shared_ptr<EventLoopThread> _eventLoopThread;
    FileDescriptor _fd;

public:
    int type;
    int events;
    int revents;
    void updateEvents();

    void onEvents(uint64_t events) {
        // if constexpr (std::same_as<Configuration::PollingContext, EpollContext>) {
        printf("WTF, I'M EPOLLCONTEXT!\n");
        int realEvents = (int)events;
        if ((realEvents & EPOLLIN) == EPOLLIN) {
            onRead();
        } else if ((realEvents & EPOLLOUT) == EPOLLOUT) {
            onWrite();
        } else if ((realEvents & EPOLLHUP) == EPOLLHUP) {
            onClose();
        } else if ((realEvents & EPOLLERR)) {
            onError();
        }
        // }
    }

    // User that registered them should have everything they need
    // In the future, we might add more onXX() methods, for now these are all we need.
    using OnEventCallback = std::function<void()>;
    OnEventCallback onRead;
    OnEventCallback onWrite;
    OnEventCallback onClose;
    OnEventCallback onError;
    // EventManager(): _fd {} {}
    EventManager(std::shared_ptr<EventLoopThread>);
    ~EventManager() {}
};
