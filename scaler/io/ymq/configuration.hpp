#pragma once

// First-party
#include "epoll_context.hpp"

class EpollContext;

struct configuration {
    using polling_context_t = EpollContext;
};
