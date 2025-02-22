#pragma once

// cffi generates C code, which doesn't support
// C++'s enum class, so we need to use plain enums
#ifdef TYPECK
#define ENUM enum class
#else
#define ENUM enum
#endif

// C
#include <cstddef>
#include <cstdint>
#include <cstring>

// C++
#include <string>
#include <iostream>
#include <source_location>
#include <semaphore>

// System
#include <sys/timerfd.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <poll.h>
#include <signal.h>

// 5ca1ab1e = scalable
static uint8_t MAGIC[4] = {0x5c, 0xa1, 0xab, 0x1e};

// Python callback
void future_set_result(void *future, void *data);

// this is an unrecoverable error that exits the program
// prints a message plus the source location
[[noreturn]] void panic(
    [[maybe_unused]] std::string message,
    const std::source_location &location = std::source_location::current())
{

    auto file_name = std::string(location.file_name());
    file_name = file_name.substr(file_name.find_last_of("/") + 1);

    std::cout << "panic at " << file_name << ":" << location.line()
              << ":" << location.column() << " in function ["
              << location.function_name() << "] in file ["
              << location.file_name() << "]: " << message << std::endl;

    exit(1);
}

struct Bytes
{
    uint8_t *data;
    size_t len;

    bool operator==(const Bytes &other) const
    {
        if (len != other.len)
            return false;

        return std::memcmp(data, other.data, len) == 0;
    }

    // same as empty()
    bool operator!() const
    {
        return len == 0 || data == nullptr;
    }

    bool empty() const
    {
        return len == 0 || data == nullptr;
    }

    // debugging utility
    std::string as_string() const
    {
        if (empty())
        {
            return "[EMPTY]";
        }

        return std::string((char *)data, len);
    }
};

struct Message
{
    // the address the message was received from, or to send to
    //
    // for received messages, the address data is
    // owned by the peer it was received from
    Bytes address;

    // the payload of the message
    //
    // for received messages, the payload data is owned
    // and must be freed when the message is destroyed
    Bytes payload;
};

// free a received message
void message_destroy(Message &msg)
{
    // the address is owned by the Peer
    free(msg.payload.data);
}

void serialize_u32(uint32_t x, uint8_t buffer[4])
{
    buffer[0] = x & 0xFF;
    buffer[1] = (x >> 8) & 0xFF;
    buffer[2] = (x >> 16) & 0xFF;
    buffer[3] = (x >> 24) & 0xFF;
}

void deserialize_u32(const uint8_t buffer[4], uint32_t *x)
{
    *x = buffer[0] | buffer[1] << 8 | buffer[2] << 16 | buffer[3] << 24;
}

typedef uint64_t timerfd_t;

// timerfd analogue of eventfd_read()
// 0 -> ok, read the value from the buffer
// -1 -> error, check errno
int timerfd_read(int fd, uint8_t *buffer)
{
    auto n = read(fd, buffer, sizeof(timerfd_t));

    if (n > 0)
    {
        if (n != sizeof(timerfd_t))
            panic("failed to read timerfd: " + std::to_string(errno));

        return 0;
    }

    return -1;
}

// read a timerfd value and discard it
// return value is the same as timerfd_read()
int timerfd_read2(int fd)
{
    timerfd_t value;
    return timerfd_read(fd, (uint8_t *)&value);
}

int eventfd_wait(int fd)
{
    eventfd_t value;
    return eventfd_read(fd, &value);
}

int eventfd_signal(int fd)
{
    return eventfd_write(fd, 1);
}

int eventfd_reset(int fd)
{
    for (;;)
    {
        if (eventfd_wait(fd) < 0)
        {
            if (errno == EAGAIN)
                return 0;

            return -1;
        }
    }
}

enum FdWait : int8_t
{
    Ready = 0,
    FdTimeout = -1,
    Other = -2,
};

// wait for an event on a file descriptor, or for a signal to arrive, possibly with a timeout
//
// fd: the file descriptor to wait on
// timeout: te number of milliseconds to wait, or -1 to wait indefinitely
// events: the events to wait for, e.g. POLLIN, POLLOUT -- passed directly to poll()
//
// return value:
//  - Fdwait::Ready (0): the file descriptor is ready
//  - Fdwait::Timeout (-1): the timeout expired
//  - Fdwait::Other (-2): poll failed for some other reason
//  - (>0): a signal was received, the value is the negative of the signal number
int8_t fd_wait(int fd, int timeout, short int events)
{
    pollfd fds[2];

    fds[0] = {
        .fd = fd,
        .events = events,
        .revents = 0,
    };

    sigset_t sigs;
    sigemptyset(&sigs);
    sigaddset(&sigs, SIGINT);
    sigaddset(&sigs, SIGQUIT);
    sigaddset(&sigs, SIGTERM);

    auto signal_fd = signalfd(-1, &sigs, 0);

    fds[1] = {
        .fd = signal_fd,
        .events = POLLIN,
        .revents = 0,
    };

    auto n = poll(fds, 2, timeout);

    if (n == 0)
    {
        close(signal_fd);
        return (int8_t)FdWait::FdTimeout;
    }

    if (n < 0)
    {
        close(signal_fd);
        return (int8_t)FdWait::Other;
    }

    if (fds[1].revents & POLLIN)
    {
        signalfd_siginfo info;

        if (read(signal_fd, &info, sizeof(info)) != sizeof(info))
        {
            panic("failed to read signalfd: " + std::to_string(errno));
        }

        close(signal_fd);
        return info.ssi_signo;
    }

    close(signal_fd);
    return 0;
}

struct Completer
{
    ENUM Type{
        None,
        Future,
        Semaphore} type;

    union
    {
        void *future_ptr;
        std::binary_semaphore *sem;
    };

    // complete with a result
    // may be NULL
    void complete(void *result);

    static constexpr Completer none()
    {
        return {
            .type = Type::None,
            .future_ptr = nullptr,
        };
    }

    static Completer future(void *future)
    {
        return {
            .type = Type::Future,
            .future_ptr = future,
        };
    }

    static Completer semaphore(std::binary_semaphore *sem)
    {
        return {
            .type = Type::Semaphore,
            .sem = sem,
        };
    }
};

void Completer::complete(void *result = NULL)
{
    switch (this->type)
    {
    case Completer::Type::None:
        break;
    case Completer::Type::Future:
        future_set_result(this->future_ptr, result);
        break;
    case Completer::Type::Semaphore:
        this->sem->release();
        break;
    }
}

ENUM IoProgress : uint8_t{
                      Magic,   // the magic is being read
                      Header,  // the header is being read
                      Payload, // the payload is being read
                  };

// an in-progress io operation
struct IoOperation
{
    IoProgress progress;
    Completer completer;
    size_t cursor;

    union
    {
        uint8_t buffer[4];
        Bytes payload;
    };

    bool completed() const
    {
        return progress == IoProgress::Payload && cursor == payload.len;
    }

    static IoOperation read(Completer completer = Completer::none())
    {
        return {
            .progress = IoProgress::Magic,
            .completer = completer,
            .cursor = 0,
            .buffer = {0},
        };
    }

    static IoOperation write(Bytes payload, Completer completer = Completer::none())
    {
        return {
            .progress = IoProgress::Magic,
            .completer = completer,
            .cursor = 0,
            .payload = payload,
        };
    }
};
