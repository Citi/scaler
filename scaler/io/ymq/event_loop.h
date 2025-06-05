#pragma once

// First-party
#include "epoll_context.h"
#include "event_manager.h"
#include "interruptive_concurrent_queue.h"
#include "timed_concurrent_queue.h"

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

    EventLoopBackend* eventLoopBackend;

    void registerEventManager(EventManager& em) { eventLoopBackend->registerEventManager(em); }

    void removeEventManager(EventManager& em) { eventLoopBackend->removeEventManager(em); }

    // void loop();
    // void stop();

    // void executeNow(Function func);
    // void executeLater(Function func, Identifier identifier);
    // void executeAt(TimeStamp, Function, Identifier identifier);
    // void cancelExecution(Identifier identifier);

    // InterruptiveConcurrentQueue<FunctionType> immediateExecutionQueue;
    // TimedConcurrentQueue<FunctionType> timedExecutionQueue;
    // ConcurrentQueue<FunctionType> delayedExecutionQueue;
};
