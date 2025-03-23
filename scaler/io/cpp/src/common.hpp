#pragma once

// cffi generates C code, which doesn't like C++'s enum class
// so we use plain enums during compilation
// you can set the TYPECK define in your IDE for safer enum typing
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

// System
#include <sys/timerfd.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <poll.h>
#include <signal.h>
#include <semaphore.h>
#include <unistd.h>

// Python callback
void future_set_result(void *future, void *data);

// this is an unrecoverable error that exits the program
// prints a message plus the source location
[[noreturn]] void panic(
    std::string message,
    const std::source_location &location = std::source_location::current())
{
    auto file_name = std::string(location.file_name());
    file_name = file_name.substr(file_name.find_last_of("/") + 1);

    std::cout << "panic at " << file_name << ":" << location.line()
              << ":" << location.column() << " in function ["
              << location.function_name() << "]: "
              << message << std::endl;

    std::abort();
}

// how to control flow
// continue is truthy
// break is falsy
struct ControlFlow
{
    enum Value
    {
        Continue,
        Break
    } value;

    constexpr ControlFlow(Value value) : value(value) {}
    constexpr operator Value() const { return value; }

    operator bool() const
    {
        switch (this->value)
        {
        case Continue:
            return true;
        case Break:
            return false;
        }

        panic("unreachable");
    }
};

uint8_t *datadup(const uint8_t *data, size_t len)
{
    uint8_t *dup = (uint8_t *)std::malloc(len);
    std::memcpy(dup, data, len);
    return dup;
}

struct Bytes
{
    uint8_t *data;
    size_t len;

    void free()
    {
        if (is_empty())
            return;

        std::free(data);
    }

    bool operator==(const Bytes &other) const
    {
        if (len != other.len)
            return false;

        return std::memcmp(data, other.data, len) == 0;
    }

    bool operator!() const
    {
        return is_empty();
    }

    bool is_empty() const
    {
        return this->data == NULL;
    }

    // debugging utility
    std::string as_string() const
    {
        if (is_empty())
            return "[EMPTY]";

        return std::string((char *)data, len);
    }

    std::string to_hex()
    {
        char *hex = (char *)std::malloc((len * 3 + 1) * sizeof(char));
        for (size_t i = 0; i < len; i++)
            sprintf(hex + i * 3, "%02x ", this->data[i]);

        hex[len * 3] = '\0';

        std::string owned(hex, len * 3);
        std::free(hex);
        return owned;
    }

    // debugging utility
    std::string hexhash()
    {
        uint64_t hash = this->easy_hash();
        auto bytes = Bytes{
            .data = (uint8_t *)&hash,
            .len = 64 / 8};

        return bytes.to_hex();
    }

    uint64_t easy_hash()
    {
        uint64_t hash = 0;
        for (size_t i = 0; i < len; i++)
            hash = (hash << 5) - hash + data[i];

        return hash;
    }

    Bytes ref()
    {
        return {
            .data = this->data,
            .len = this->len};
    }

    static Bytes alloc(size_t len)
    {
        return {
            .data = (uint8_t *)std::malloc(len),
            .len = len};
    }

    static Bytes empty()
    {
        return {
            .data = NULL,
            .len = 0,
        };
    }

    static Bytes copy(const uint8_t *data, size_t len)
    {
        return {
            .data = datadup(data, len),
            .len = len,
        };
    }

    static Bytes clone(const Bytes &bytes)
    {
        if (bytes.is_empty())
            panic("tried to clone empty bytes");

        return {
            .data = datadup(bytes.data, bytes.len),
            .len = bytes.len,
        };
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

// free a message's resources
void message_destroy(Message *msg)
{
    msg->payload.free();
    msg->address.free();
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
//  - Fdwait::Other (-2): poll failed for some other reason; check errno
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
        sem_t *sem;
    };

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

    static Completer semaphore(sem_t *sem)
    {
        return {
            .type = Type::Semaphore,
            .sem = sem,
        };
    }

    // complete with a result
    // may be NULL
    void complete(void *result = NULL)
    {
        switch (this->type)
        {
        case Completer::Type::None:
            break;
        case Completer::Type::Future:
            future_set_result(this->future_ptr, result);
            break;
        case Completer::Type::Semaphore:
            if (sem_post(this->sem) < 0)
                panic("failed to post semaphore: " + std::to_string(errno));
            break;
        }
    }
};

ENUM IoProgress : uint8_t{Header, Payload};

// an in-progress io operation
struct IoOperation
{
    IoProgress progress;
    Completer completer;
    size_t cursor;

    uint8_t buffer[4];
    Bytes payload;

    bool completed() const
    {
        return progress == IoProgress::Payload && cursor == payload.len;
    }

    void complete(void *result = NULL)
    {
        completer.complete(result);
    }

    static IoOperation read(Completer completer = Completer::none())
    {
        return {
            .progress = IoProgress::Header,
            .completer = completer,
            .cursor = 0,
            .buffer = {0},
            .payload = Bytes::empty()};
    }

    // the payload must live at least as long as the operation does
    static IoOperation write(Bytes payload, Completer completer = Completer::none())
    {
        return {
            .progress = IoProgress::Header,
            .completer = completer,
            .cursor = 0,
            .buffer = {0},
            .payload = payload,
        };
    }
};
