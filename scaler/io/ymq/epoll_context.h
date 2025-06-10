#pragma once

// First-party
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/file_descriptor.h"

class EventManager;

struct EpollContext {
    FileDescriptor epoll_fd;

    void registerEventManager(EventManager& em);
    void removeEventManager(EventManager& em);
};
