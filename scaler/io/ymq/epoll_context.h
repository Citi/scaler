#pragma once

// System
#include "sys/epoll.h"

// First-party
#include "event_manager.h"
#include "file_descriptor.h"

class EventManager;

struct EpollContext {
    FileDescriptor epoll_fd;

    void registerEventManager(EventManager& em) {
        epoll_event ev {
            .events = EPOLLOUT | EPOLLIN | EPOLLET,  // Edge-triggered
            .data = {.ptr = &em},
        };

        epoll_fd.epoll_ctl(EPOLL_CTL_ADD, em._fd, &ev);
    }

    void removeEventManager(EventManager& em) {
        epoll_fd.epoll_ctl(EPOLL_CTL_DEL, em._fd, nullptr);
    }
};
