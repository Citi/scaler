#pragma once

// System
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

// C
#include <cassert>
#include <cerrno>

// C++
#include <expected>
#include <optional>

// First-party
#include "common.h"

class FileDescriptor {
    enum class Ownership { Owned, Borrowed } ownership;
    int fd;

    FileDescriptor(int fd): fd(fd) {}

    void assert_owned() const { assert(ownership == Ownership::Owned); }

public:
    ~FileDescriptor() noexcept(false) {
        if (this->ownership == Ownership::Owned)
            if (auto code = close(fd) < 0)
                throw std::system_error(errno, std::system_category(), "Failed to close file descriptor");

        this->fd = -1;
    }

    FileDescriptor(const FileDescriptor& other) {
        this->fd        = other.fd;
        this->ownership = Ownership::Borrowed;
    }

    FileDescriptor& operator=(const FileDescriptor& other) {
        if (this->ownership == Ownership::Owned) {
            if (fd >= 0)
                close(fd);
        }

        this->fd        = other.fd;
        this->ownership = Ownership::Borrowed;

        return *this;
    }

    FileDescriptor(FileDescriptor&& other) noexcept: fd(other.fd) {
        other.fd = -1;  // prevent double close
    }

    FileDescriptor& operator=(FileDescriptor&& other) noexcept {
        if (this != &other) {
            if (fd >= 0)
                close(fd);
            this->fd        = other.fd;
            other.fd        = -1;  // prevent double close
            this->ownership = other.ownership;
            other.ownership = Ownership::Borrowed;  // transfer ownership
        }
        return *this;
    }

    bool operator==(const FileDescriptor& other) const { return fd == other.fd; }

    static std::expected<FileDescriptor, Errno> socket(int domain, int type, int protocol) {
        if (int fd = ::socket(domain, type, protocol) < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }

    static std::expected<FileDescriptor, Errno> eventfd(int initval, int flags) {
        if (int fd = ::eventfd(initval, flags) < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }

    static std::expected<FileDescriptor, Errno> timerfd(clockid_t clock, int flags) {
        if (int fd = ::timerfd_create(clock, flags) < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }

    static std::expected<FileDescriptor, Errno> epollfd() {
        if (int fd = ::epoll_create1(0) < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }

    std::optional<Errno> listen(int backlog) {
        assert_owned();

        if (::listen(fd, backlog) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::expected<FileDescriptor, Errno> accept(sockaddr& addr, socklen_t& addrlen) {
        assert_owned();

        if (auto fd2 = ::accept(fd, &addr, &addrlen) < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd2);
        }
    }

    std::optional<Errno> connect(const sockaddr& addr, socklen_t addrlen) {
        assert_owned();

        if (::connect(fd, &addr, addrlen) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> bind(const sockaddr& addr, socklen_t addrlen) {
        assert_owned();

        if (::bind(fd, &addr, addrlen) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::expected<ssize_t, Errno> read(void* buf, size_t count) {
        assert_owned();

        ssize_t n = ::read(fd, buf, count);
        if (n < 0) {
            return std::unexpected {errno};
        } else {
            return n;
        }
    }

    std::expected<ssize_t, Errno> write(const void* buf, size_t count) {
        assert_owned();

        ssize_t n = ::write(fd, buf, count);
        if (n < 0) {
            return std::unexpected {errno};
        } else {
            return n;
        }
    }

    std::optional<Errno> eventfd_signal() {
        assert_owned();

        uint64_t u = 1;
        if (::eventfd_write(fd, u) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> eventfd_wait() {
        assert_owned();

        uint64_t u;
        if (::eventfd_read(fd, &u) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> timerfd_settime(const itimerspec& new_value, itimerspec* old_value = nullptr) {
        assert_owned();

        if (::timerfd_settime(fd, 0, &new_value, old_value) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> timerfd_wait() {
        assert_owned();

        uint64_t u;
        if (::read(fd, &u, sizeof(u)) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> epoll_ctl(int op, FileDescriptor& other, epoll_event* event) {
        assert_owned();

        if (::epoll_ctl(fd, op, other.fd, event) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> epoll_wait(epoll_event* events, int maxevents, int timeout) {
        assert_owned();

        if (::epoll_wait(fd, events, maxevents, timeout) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }
};
