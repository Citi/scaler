#include "scaler/io/ymq/epoll_context.h"

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
    while (delayedFunctions.size()) {
        auto top = delayedFunctions.front();
        top();
        delayedFunctions.pop();
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
    printf("hereinloop\n");
    std::array<epoll_event, 1024> events;
    int n = epoll_wait(epfd, events.data(), 1024, -1);
    if (n < 0) {
        //     // TODO: handle error
        printf("?\n");
    } else {
        printf("n = %d\n", n);
        sleep(1);
    }

    for (int ii = 0; ii < n; ++ii) {
        // epoll_event current_event = *it;
        epoll_event current_event = events[ii];
        auto* event               = (EventManager*)current_event.data.ptr;
        printf("event->type = %d\n", event->type);
        if (event->type == 123) {
            printf("1\n");
            std::function<void()> somefunc = [] { printf("SOMEFUNC\n"); };
            printf("2\n");
            interruptiveFunctions.dequeue(somefunc);
            printf("3\n");
            somefunc();
            printf("4\n");
            continue;
        }
        // } else {
        //     printf("type != -1\n");
        //     event->onEvents(current_event.events);
        // }
    }

    // execPendingFunctions();
}

void EpollContext::addFdToLoop(int fd, uint64_t events, EventManager* manager) {
    epoll_event event {};
    event.events   = (int)events & (EPOLLIN | EPOLLOUT | EPOLLET);
    event.data.ptr = (void*)manager;
    int res        = epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event);

    if (res < 0) {
        printf("?\n");
        exit(1);
    }
}

// EXAMPLE
// epoll_wait;
// for each event that is returned to the caller {
//     cast the event back to EventManager
//     if this event is an eventfd {
//         func = queue front
//         func()
//     } else if this event is an timerfd {
//         if it is oneshot then execute once
//         if it is multishot then execute and rearm timer
//     } else {
//         eventmanager.revent = events return by epoll
//         eventmanager.on_event() ,
//         where as on_event is set differently for tcpserver, tcpclient, and tcpconn

//         they are defined something like:
//         tcpserver.on_event() {
//             accept the socket and generate a new tcpConn or handle err
//             this.ioSocket.addNewConn(tcpConn)
//         }
//         tcpclient.on_event() {
//             connect the socket and generate a new tcpConn
//             this.ioSocket.addNewConn(tcpConn)
//             if there need retry {
//                 close this socket
//                 this.eventloop.executeAfter(the time you want from now)
//             }
//         }
//         tcpConn.on_event() {
//             read everything you can to the buffer
//             write everything you can to write the remote end
//             if tcpconn.ioSocket is something special, for example dealer
//             tcpConn.ioSocket.route to corresponding tcpConn
//         }
//
//     }

// }
