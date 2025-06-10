#include "scaler/io/ymq/epoll_context.h"

void EpollContext::registerEventManager(EventManager& em) {
    epoll_event ev {
        .events = EPOLLOUT | EPOLLIN | EPOLLET,  // Edge-triggered
        .data   = {.ptr = &em},
    };

    epoll_fd.epoll_ctl(EPOLL_CTL_ADD, em._fd, &ev);
}

void EpollContext::removeEventManager(EventManager& em) {
    epoll_fd.epoll_ctl(EPOLL_CTL_DEL, em._fd, nullptr);
}
