#pragma once

// System
#include <sys/epoll.h>

// C++
#include <functional>
#include <queue>
#include <system_error>

#include "scaler/io/ymq/timed_queue.h"

// First-party
#include "scaler/io/ymq/file_descriptor.h"
#include "scaler/io/ymq/interruptive_concurrent_queue.h"
#include "scaler/io/ymq/timestamp.h"

class EventManager;

// struct EpollContext {
//     FileDescriptor epoll_fd;
//     TimedQueue timingFunctions;
//
//     using DelayedFunctionQueue = std::queue<std::function<void()>>;
//     DelayedFunctionQueue delayedFunctions;
//
//     using Function   = std::function<void()>;  // TBD
//     using Identifier = int;                    // TBD
//     void registerCallbackBeforeLoop(EventManager*);
//
//     EpollContext() {
//         auto fd = FileDescriptor::epollfd();
//
//         if (!fd) {
//             throw std::system_error(fd.error(), std::system_category(), "Failed to create epoll fd");
//         }
//
//         this->epoll_fd = std::move(*fd);
//         timingFunctions.onCreated();
//     }
//
//     void loop();
//     void registerEventManager(EventManager& em);
//     void removeEventManager(EventManager& em);
//
//     void stop();
//
//     void executeNow(Function func) {
//     }
//
//     void executeLater(Function func, Identifier) { delayedFunctions.emplace(std::move(func)); }
//
//     void executeAt(Timestamp timestamp, Function callback) { timingFunctions.push(timestamp, callback); }
//
//     bool cancelExecution(Identifier identifier);
//
//     void execPendingFunctions();
//
//     void addFdToLoop(int fd, uint64_t events, EventManager* manager);
//
//     // int connect_timer_tfd;
//     // std::map<int, EventManager*> monitoringEvent;
//     // bool timer_armed;
//     // // NOTE: Utility functions, may be defined otherwise
//     // void ensure_timer_armed();
//     // void remove_epoll(int fd);
//     // EpollData* epoll_by_fd(int fd);
// };

using DelayedFunctionQueue = std::queue<std::function<void()>>;
using Function             = std::function<void()>;

class EpollContext {
    int _epfd;
    TimedQueue _timingFunctions;
    DelayedFunctionQueue _delayedFunctions;
    InterruptiveConcurrentQueue<std::function<void()>> _interruptiveFunctions;

public:
    using Identifier = int;  // TBD

    // TODO: This is obviously not the right way of doing it
    EpollContext() {
        _epfd = epoll_create1(0);

        epoll_event event;
        event.events   = EPOLLIN | EPOLLET;
        event.data.ptr = _interruptiveFunctions._eventManager.get();

        epoll_ctl(_epfd, EPOLL_CTL_ADD, _interruptiveFunctions.eventFd(), &event);
    }

    void loop();
    void stop();

    void registerEventManager(EventManager& em);
    void removeEventManager(EventManager& em);

    void executeNow(Function func) { _interruptiveFunctions.enqueue(func); }
    void executeLater(Function func, Identifier) { _delayedFunctions.emplace(std::move(func)); }
    void executeAt(Timestamp timestamp, Function callback) { _timingFunctions.push(timestamp, callback); }
    // TODO: figure out how this work with existing util
    bool cancelExecution(Identifier identifier);

    void execPendingFunctions();

    void addFdToLoop(int fd, uint64_t events, EventManager* manager);
};
