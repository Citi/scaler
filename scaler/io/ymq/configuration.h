#pragma once

// First-party
#include "scaler/io/ymq/epoll_context.h"

class EpollContext;

struct configuration {
    using polling_context_t = EpollContext;
    using Identity = std::string;
};
