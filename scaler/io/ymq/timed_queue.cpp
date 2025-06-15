
#include "scaler/io/ymq/timed_queue.h"

#include <sys/epoll.h>

TimedQueue::TimedQueue(): timer_fd(createTimerfd()) {}

void TimedQueue::push(Timestamp timestamp, callback_t cb) {
    if (pq.size() && timestamp < pq.top().first) {
        auto ts = convertToItimerspec(timestamp);
        int ret = timerfd_settime(timer_fd, 0, &ts, nullptr);
    }

    pq.push({timestamp, cb});
}

void TimedQueue::onCreated() {}
