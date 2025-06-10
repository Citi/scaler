#pragma once

// C++
#include <string>

class EpollContext;

struct configuration {
    using polling_context_t = EpollContext;
    using Identity = std::string;
};
