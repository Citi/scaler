#pragma once

// System
#include <__expected/unexpected.h>
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
#include <memory>
#include <optional>

// First-party
#include "scaler/io/ymq/common.h"

class FileDescriptor {
    std::shared_ptr<int> _fd;

    // note: not allowed to throw exceptions
    static void deleter(int* fd) { ::close(*fd); }

    int fd() const { return *_fd; }

    FileDescriptor(int fd): _fd(std::shared_ptr<int>(new int(fd), &FileDescriptor::deleter)) {}

public:
    FileDescriptor(const FileDescriptor& other) { this->_fd = other._fd; }

    FileDescriptor(): FileDescriptor(-1) {}

    FileDescriptor& operator=(const FileDescriptor& other) {
        this->_fd = other._fd;
        return *this;
    }

    bool operator==(const FileDescriptor& other) const { return *_fd == other.fd(); }

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
        if (::listen(*_fd, backlog) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::expected<FileDescriptor, Errno> accept(sockaddr& addr, socklen_t& addrlen) {
        if (auto fd2 = ::accept(*_fd, &addr, &addrlen) < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd2);
        }
    }

    std::optional<Errno> connect(const sockaddr& addr, socklen_t addrlen) {
        if (::connect(*_fd, &addr, addrlen) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> bind(const sockaddr& addr, socklen_t addrlen) {
        if (::bind(*_fd, &addr, addrlen) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::expected<ssize_t, Errno> read(void* buf, size_t count) {
        ssize_t n = ::read(*_fd, buf, count);
        if (n < 0) {
            return std::unexpected {errno};
        } else {
            return n;
        }
    }

    std::expected<ssize_t, Errno> write(const void* buf, size_t count) {
        ssize_t n = ::write(*_fd, buf, count);
        if (n < 0) {
            return std::unexpected {errno};
        } else {
            return n;
        }
    }

    std::optional<Errno> eventfd_signal() {
        uint64_t u = 1;
        if (::eventfd_write(*_fd, u) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> eventfd_wait() {
        uint64_t u;
        if (::eventfd_read(*_fd, &u) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> timerfd_settime(const itimerspec& new_value, itimerspec* old_value = nullptr) {
        if (::timerfd_settime(*_fd, 0, &new_value, old_value) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> timerfd_wait() {
        uint64_t u;
        if (::read(*_fd, &u, sizeof(u)) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> epoll_ctl(int op, FileDescriptor& other, epoll_event* event) {
        if (::epoll_ctl(*_fd, op, *other._fd, event) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::expected<int, Errno> epoll_wait(epoll_event* events, int maxevents, int timeout) {
        if (auto n = ::epoll_wait(*_fd, events, maxevents, timeout) < 0) {
            return std::unexpected {errno};
        } else {
            return n;
        }
    }
};
