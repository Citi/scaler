#pragma once

// C++
#include <functional>

// First-party
#include "event_manager.hpp"
#include "interruptive_concurrent_queue.hpp"
#include "timed_concurrent_queue.hpp"
#include "epoll_context.hpp"

// Third-Party
#include "third_party/concurrentqueue.h"

using moodycamel::ConcurrentQueue;

class EpollContext;
class EventManager;

template <class EventLoopBackend = EpollContext>
struct EventLoop {
    using Function   = std::function<void()>;  // TBD
    using TimeStamp  = int;                    // TBD
    using Identifier = int;                    // TBD
    void loop();
    void stop();

    void executeNow(Function func);
    void executeLater(Function func, Identifier identifier);
    void executeAt(TimeStamp, Function, Identifier identifier);
    void cancelExecution(Identifier identifier);

    void registerEventManager(EventManager& em) {
        eventLoopBackend->registerEventManager(em);
    }

    void removeEventManager(EventManager& em) {
        eventLoopBackend->removeEventManager(em);
    }

    InterruptiveConcurrentQueue<Function> immediateExecutionQueue;
    TimedConcurrentQueue<Function> timedExecutionQueue;
    ConcurrentQueue<Function> delayedExecutionQueue;

    EventLoopBackend* eventLoopBackend;
};
