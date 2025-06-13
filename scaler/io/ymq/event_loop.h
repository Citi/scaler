#pragma once

// C++
#include <functional>

// First-party
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/epoll_context.h"

struct Timestamp;
class EventManager;

template <typename EventLoopBackend = EpollContext>
struct EventLoop {
    using Function   = std::function<void()>;  // TBD
    using Identifier = int;                    // TBD
    void loop() { eventLoopBackend->loop(); }
    void stop();

    void executeNow(Function func) { eventLoopBackend->executeNow(func); }
    void executeLater(Function func, Identifier identifier);
    void executeAt(Timestamp, Function, Identifier identifier);
    void cancelExecution(Identifier identifier);

    void registerEventManager(EventManager& em) { eventLoopBackend->registerEventManager(em); }
    void removeEventManager(EventManager& em) { eventLoopBackend->removeEventManager(em); }

    // InterruptiveConcurrentQueue<FunctionType> immediateExecutionQueue;
    // TimedConcurrentQueue<FunctionType> timedExecutionQueue;
    // ConcurrentQueue<FunctionType> delayedExecutionQueue;

    EventLoopBackend* eventLoopBackend;
};
