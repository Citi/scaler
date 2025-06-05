// System
#include <sys/eventfd.h>

// C++
#include <optional>

// First-party
#include "common.h"
#include "file_descriptor.h"

// Third-party
#include "third_party/concurrentqueue.h"

using moodycamel::ConcurrentQueue;

template <typename T>
class InterruptiveConcurrentQueue {
    ConcurrentQueue<T> _queue;
    FileDescriptor _eventFd;

public:
    InterruptiveConcurrentQueue(): _queue() {
        auto fd = FileDescriptor::eventfd(0, EFD_SEMAPHORE);

        if (!fd) {
            throw std::system_error(fd.error(), std::system_category(), "Failed to create eventfd");
        }

        _eventFd = std::move(*fd);
    }

    // unmovable, uncopyable
    InterruptiveConcurrentQueue(const InterruptiveConcurrentQueue&)            = delete;
    InterruptiveConcurrentQueue& operator=(const InterruptiveConcurrentQueue&) = delete;
    InterruptiveConcurrentQueue(InterruptiveConcurrentQueue&&)                 = delete;
    InterruptiveConcurrentQueue& operator=(InterruptiveConcurrentQueue&&)      = delete;

    // returns a non-owned file descriptor
    FileDescriptor eventFd() const {
        return _eventFd;
    }

    void enqueue(const T& item) {
        _queue.enqueue(item);
        _eventFd.eventfd_signal();
    }

    // note: this method will block until an item is available
    std::optional<Errno> dequeue(T& item) {
        if (auto result = _eventFd.eventfd_wait(); !result) {
            // If the eventfd wait failed, we return false
            return result;
        }

        for (;;) {
            if (_queue.try_dequeue(item)) {
                return std::nullopt;  // success
            }
        }
    }
};
