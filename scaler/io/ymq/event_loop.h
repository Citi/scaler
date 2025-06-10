#pragma once

// First-party
#include "scaler/io/ymq/epoll_context.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/interruptive_concurrent_queue.h"
#include "scaler/io/ymq/timed_concurrent_queue.h"

// Third-Party
#include "scaler/io/ymq/third_party/concurrentqueue.h"

using moodycamel::ConcurrentQueue;

class EpollContext;
class EventManager;

template <typename T>
class InterruptiveConcurrentQueue;

template <class EventLoopBackend = EpollContext>
struct EventLoop {
    using Function   = std::function<void()>;  // TBD
    using TimeStamp  = int;                    // TBD
    using Identifier = int;                    // TBD

    EventLoopBackend* eventLoopBackend;
    InterruptiveConcurrentQueue<Function>* immediateExecutionQueue;

    EventLoop(EventLoopBackend& backend)
        : eventLoopBackend(backend), immediateExecutionQueue(eventLoopBackend, [](Function func) { func(); }) {}

    void registerEventManager(EventManager& em) { eventLoopBackend->registerEventManager(em); }

    void removeEventManager(EventManager& em) { eventLoopBackend->removeEventManager(em); }

    // void loop();
    // void stop();

    void executeNow(Function func) { immediateExecutionQueue->enqueue(std::move(func)); }
    // void executeLater(Function func, Identifier identifier);
    // void executeAt(TimeStamp, Function, Identifier identifier);
    // void cancelExecution(Identifier identifier);

    // InterruptiveConcurrentQueue<FunctionType> immediateExecutionQueue;
    // TimedConcurrentQueue<FunctionType> timedExecutionQueue;
    // ConcurrentQueue<FunctionType> delayedExecutionQueue;
};
