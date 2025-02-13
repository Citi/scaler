#pragma once

// C++
#include <atomic>
#include <shared_mutex>
#include <vector>
#include <queue>

// System
#include <sys/epoll.h>
#include <sys/timerfd.h>

// First-party
#include "intra_process.h"

struct EpollType {
    enum Value {
        ClientSend,
        ClientRecv,
        ClientListener,
        ClientPeerRecv,
        IntraProcessClientRecv,
        PeerConnecting,

        ConnectTimer,
        Control,
        Closed
    };

    constexpr EpollType(Value value) : value(value) {}
    constexpr operator Value() const { return value; }

    std::string as_string() const
    {
        switch (value)
        {
        case ClientSend:
            return "ClientSend";
        case ClientRecv:
            return "ClientRecv";
        case ClientListener:
            return "ClientListener";
        case ClientPeerRecv:
            return "ClientPeerRecv";
        case IntraProcessClientRecv:
            return "IntraProcessClientRecv";
        case PeerConnecting:
            return "PeerConnecting";
        case ConnectTimer:
            return "ConnectTimer";
        case Control:
            return "Control";
        case Closed:
            return "Closed";
        }
    }

    private:
    Value value;
};

// this is an in-progress write operation
// created only after the entire header has been written
struct WriteOperation
{
    SendMsg send;
    size_t cursor;
};

// an in-progress read operation
// created only after the entire header has been read
struct ReadOperation
{
    Message message;
    size_t cursor;
};

struct EpollData2
{
    int fd;
    EpollType type;
    std::optional<ReadOperation> read;
    std::optional<WriteOperation> write;

    union
    {
        void *ptr;
        Client *client;
        IntraProcessClient *inproc;
        Peer *peer;
    };
};

enum class ControlOperation
{
    AddClient,
    RemoveClient,
    Connect,
};

struct ControlRequest
{
    ControlOperation op;
    int client_fd;
    std::optional<std::binary_semaphore> sem;
    std::optional<sockaddr_storage> addr;

    union
    {
        void *data;
        Client *client;
    };
};

struct ThreadContext
{
    Session *session;
    std::thread thread;
    std::vector<EpollData2 *> io_cache;
    std::queue<Peer *> connecting;
    ConcurrentQueue<ControlRequest> control;
    int control_efd;
    int epoll_fd;
    int connect_timer_tfd;
    bool timer_armed;

    void arm_timer()
    {
        const time_t timeout_s = 3;

        itimerspec timer{
            .it_interval = {.tv_sec = 0, .tv_nsec = 0},
            .it_value = {.tv_sec = timeout_s, .tv_nsec = 0},
        };

        if (timerfd_settime(this->connect_timer_tfd, 0, &timer, nullptr) < 0)
        {
            panic("failed to arm timer");
        }
        this->timer_armed = true;
    }

    void accept_peer(Peer *peer);

    void add_client(Client *client)
    {
        this->add_epoll(client->send_event_fd, EPOLLIN | EPOLLET, EpollType::ClientSend, client);
        this->add_epoll(client->recv_event_fd, EPOLLIN | EPOLLET, EpollType::ClientRecv, client);
    }

    void remove_client(Client *client)
    {
        this->remove_epoll(client->send_event_fd);
        this->remove_epoll(client->recv_event_fd);
    }

    // must be called on io-thread
    void add_epoll(int fd, uint32_t flags, EpollType type, void *data)
    {
        auto edata = new EpollData2{
            .fd = fd,
            .type = type,
            .ptr = data,
        };

        epoll_event event{
            .events = flags,
            .data = {.ptr = edata}};

        epoll_ctl(this->epoll_fd, EPOLL_CTL_ADD, fd, &event);

        this->io_cache.push_back(edata);
    }

    // must be called on io-thread
    void remove_epoll(int fd)
    {
        if (epoll_ctl(this->epoll_fd, EPOLL_CTL_DEL, fd, nullptr) < 0)
        {
            // we ignore enoent because it means the fd was already removed
            if (errno != ENOENT)
                panic("failed to remove epoll fd: " + std::to_string(fd) + "; " + strerror(errno));
        }

        auto edata = std::find_if(this->io_cache.begin(), this->io_cache.end(), [fd](EpollData2 *d)
                                  { return d->fd == fd; });

        if (edata != this->io_cache.end())
        {
            delete *edata;
            this->io_cache.erase(edata);
        }
    }
};

struct Session
{
    // the io threads
    std::vector<ThreadContext> threads;
    std::vector<IntraProcessClient *> inprocs;

    std::shared_mutex intraprocess_mutex;

    int epoll_close_efd;

    std::atomic_uint8_t thread_rr;

    inline size_t num_threads()
    {
        return threads.size();
    };

    inline bool is_single_threaded()
    {
        return num_threads() == 1;
    };
};

// public api
void session_init(Session *session, size_t num_threads)
{
    new (session) Session{
        .threads = std::vector<ThreadContext>(),
        .inprocs = std::vector<IntraProcessClient *>(),
        .intraprocess_mutex = std::shared_mutex(),
        .epoll_close_efd = eventfd(0, 0),
    };

    // exactly size the vector to avoid reallocation
    session->threads.reserve(num_threads);

    for (size_t i = 0; i < num_threads; ++i)
    {
        session->threads.emplace_back(ThreadContext{
            // note: this does not start the thread
            .session = session,
            .thread = std::thread(),
            .io_cache = std::vector<EpollData2 *>(),
            .connecting = std::queue<Peer *>(),
            .control = ConcurrentQueue<ControlRequest>(),
            .control_efd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
            .epoll_fd = epoll_create1(0),
            .connect_timer_tfd = timerfd_create(CLOCK_MONOTONIC, 0),
            .timer_armed = false,
        });

        auto ctx = &session->threads.back();

        ctx->add_epoll(session->epoll_close_efd, EPOLLIN, EpollType::Closed, nullptr);
        ctx->add_epoll(ctx->control_efd, EPOLLIN, EpollType::Control, nullptr);
        ctx->add_epoll(ctx->connect_timer_tfd, EPOLLIN, EpollType::ConnectTimer, nullptr);

        ctx->thread = std::thread(io_thread_main, ctx, i);
    }
}
