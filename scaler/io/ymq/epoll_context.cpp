#include "scaler/io/ymq/epoll_context.h"

#include <cerrno>
#include <format>
#include <functional>

#include "scaler/io/ymq/common.h"
#include "scaler/io/ymq/event_manager.h"

// void EpollContext::registerEventManager(EventManager& em) {
//     epoll_event ev {
//         .events = EPOLLOUT | EPOLLIN | EPOLLET,  // Edge-triggered
//         .data   = {.ptr = &em},
//     };
//
//     epoll_fd.epoll_ctl(EPOLL_CTL_ADD, em._fd, &ev);
// }
//
// void EpollContext::removeEventManager(EventManager& em) {
//     epoll_fd.epoll_ctl(EPOLL_CTL_DEL, em._fd, nullptr);
// }

void EpollContext::execPendingFunctions() {
    while (_delayedFunctions.size()) {
        auto top = _delayedFunctions.front();
        top();
        _delayedFunctions.pop();
    }
}

// void EpollContext::loop() {
//     std::array<epoll_event, 1024> events;
//     auto result = epoll_fd.epoll_wait(events.data(), 1024, -1);
//
//     if (!result) {
//         panic(std::format("Failed to epoll_wait(): {}", result.error()));
//     };
//
//     for (auto it = events.begin(); it != events.begin() + *result; ++it) {
//         epoll_event current_event = *it;
//         if (current_event.events & EPOLLERR) {
//             // ...
//         }
//
//         auto* event = (EventManager*)current_event.data.ptr;
//         // event->onEvents(current_event.events);
//     }
//     execPendingFunctions();
// }

void EpollContext::loop() {
    std::array<epoll_event, 1024> events;
    int n = epoll_wait(_epfd, events.data(), 1024, -1);

    for (auto it = events.begin(); it != events.begin() + n; ++it) {
        epoll_event current_event = *it;
        auto* event               = (EventManager*)current_event.data.ptr;
        // TODO: Change the event type to more meaningful stuff
        if (event->type == 123) {
            // Handle where f is empty
            std::function<void()> f;
            _interruptiveFunctions.dequeue(f);
            f();
            continue;
        } else {
            event->onEvents(current_event.events);
        }
    }

    execPendingFunctions();
}

void EpollContext::addFdToLoop(int fd, uint64_t events, EventManager* manager) {
    epoll_event event {};
    event.events   = (int)events & (EPOLLIN | EPOLLOUT | EPOLLET);
    event.data.ptr = (void*)manager;
    int res        = epoll_ctl(_epfd, EPOLL_CTL_ADD, fd, &event);

    // TODO: This epoll_ctl_mod ideally should not happen here.
    if (res < 0) {
        if (errno == EEXIST) {
            if (epoll_ctl(_epfd, EPOLL_CTL_MOD, fd, &event) < 0) {
                printf("epoll ctl goes wrong\n");
                exit(1);
            }
        } else {
            printf("epoll ctl goes wrong\n");
            exit(1);
        }
    }
}
