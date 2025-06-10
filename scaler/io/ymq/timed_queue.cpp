
#include "scaler/io/ymq/timed_queue.h"

#include <sys/epoll.h>

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"

TimedQueue::TimedQueue(std::shared_ptr<EventLoopThread> eventLoopThread)
    : eventLoopThread(eventLoopThread), timer_fd(createTimerfd()) {}

TimedQueue::TimedQueue(): eventLoopThread(nullptr), timer_fd(createTimerfd()) {}

void TimedQueue::push(Timestamp timestamp, callback_t cb) {
    if (pq.size() && timestamp < pq.top().first) {
        auto ts = convertToItimerspec(timestamp);
        int ret = timerfd_settime(timer_fd, 0, &ts, nullptr);
    }

    pq.push({timestamp, cb});
}

void TimedQueue::onCreated() {
    eventLoopThread->eventLoop.addFdToLoop(timer_fd, EPOLLIN, events.get());
}
