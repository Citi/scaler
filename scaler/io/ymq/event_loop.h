#pragma once

// C++
#include <cstdint>  // uint64_t
#include <functional>

// First-party
#include "scaler/io/ymq/epoll_context.h"

struct Timestamp;
class EventManager;

template <class EventLoopBackend = EpollContext>
struct EventLoop {
    using Function   = std::function<void()>;  // TBD
    using Identifier = int;                    // TBD
    void loop() { eventLoopBackend.loop(); }
    void stop();

    void executeNow(Function func) { eventLoopBackend.executeNow(func); }
    // TODO: Move those function call. Perhaps using std::invoke(execX, std::forward<Function>(func));
    void executeLater(Function func, Identifier identifier) { eventLoopBackend.executeLater(func, identifier); }
    // void executeAt(Timestamp, Function, Identifier identifier);
    void executeAt(Timestamp timestamp, Function func) { eventLoopBackend.executeAt(timestamp, func); }
    bool cancelExecution(Identifier identifier);
    void registerCallbackBeforeLoop(EventManager*);

    void registerEventManager(EventManager& em) { eventLoopBackend.registerEventManager(em); }

    void addFdToLoop(int fd, uint64_t events, EventManager* manager) {
        eventLoopBackend.addFdToLoop(fd, events, manager);
    }

    EventLoopBackend eventLoopBackend;
};
