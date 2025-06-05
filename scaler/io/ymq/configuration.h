#pragma once

// First-party
#include "epoll_context.h"

class EpollContext;

struct configuration {
    using polling_context_t = EpollContext;
    using Identity = std::string;
};
