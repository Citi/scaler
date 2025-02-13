#pragma once

// C++
#include <atomic>
#include <shared_mutex>
#include <vector>
#include <queue>
#include <thread>

// System
#include <sys/epoll.h>
#include <sys/timerfd.h>

// First-party
#include "intra_process.h"

void io_thread_main(ThreadContext *ctx);

struct EpollType
{
    enum Value
    {
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
    size_t id;
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

    void start()
    {
        this->thread = std::thread(io_thread_main, this);
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
            .id = i,
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

        ctx->start();
    }
}

void io_thread_main(ThreadContext *ctx)
{
    for (;;)
    {
        epoll_event event;
        auto n_events = epoll_wait(ctx->epoll_fd, &event, 1, -1);

        if (n_events == 0)
        {
            continue;
        }

        EpollData2 *data = (EpollData2 *)event.data.ptr;

        if (data->type == EpollType::ConnectTimer)
        {
            // the timer is no longer armed
            ctx->timer_armed = false;

            // retry connecting peers
            // read the timer fd
            {
                uint64_t value;
                auto n = read(ctx->connect_timer_tfd, &value, sizeof(value));

                if (n < 0)
                {
                    if (errno == EAGAIN)
                        break;

                    panic("failed to read from connect timer");
                }

                if (n != sizeof(value))
                    panic("failed to read from connect timer");
            }

            while (!ctx->connecting.empty())
            {
                auto peer = ctx->connecting.front();
                ctx->connecting.pop();

                client_connect_request(ctx, peer);
            }

            continue;
        }

        if (data->type == EpollType::Control)
        {
            // a control request has been received
            ControlRequest request;
            while (!ctx->control.try_dequeue(request))
                std::this_thread::yield();

            switch (request.op)
            {
            case ControlOperation::AddClient:
                ctx->add_client(request.client);
                break;
            case ControlOperation::RemoveClient:
                ctx->remove_client(request.client);
                break;
            case ControlOperation::Connect:
            {
                auto peer = new Peer{
                    // a real fd will be assigned later
                    .fd = -1,
                    .client = request.client,
                    .addr = *request.addr,
                };

                client_connect_request(ctx, peer);
            }
            break;
            }

            if (request.sem)
                request.sem->release();

            continue;
        }

        if (data->type == EpollType::PeerConnecting)
        {
            // the peer connection was in progress and has become writeable
            ctx->remove_epoll(data->fd);

            int result;
            socklen_t result_len;
            if (getsockopt(data->fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
            {
                close(data->fd);
                delete data->peer;

                panic("failed to get socket error");
            }

            if (result == 0)
            {
                // success, peer is connected
                ctx->remove_epoll(data->fd);

                // add the peer to the client's list
                data->peer->client->peers.push_back(data->peer);
                ctx->accept_peer(data->peer);
            }
            else
            {
                // todo: are there other recoverable errors?
                if (result == ECONNREFUSED || result == ETIMEDOUT)
                {
                    ctx->connecting.push(data->peer);

                    // don't bother arming the timer if it's already armed
                    if (!ctx->timer_armed)
                        ctx->arm_timer();
                }

                panic("failed to connect to peer: " + std::to_string(result));
            }

            continue;
        }

        if (data->type == EpollType::Closed)
        {
            // the session is closing
            // todo: cleanup logic(?)
            return;
        }

        if (event.events & EPOLLIN)
        {
            // read event

            // clang-format off
            switch (data->type)
            {
                // the client has issued a send() call
                case EpollType::ClientSend:             client_send_event(data->client);            break;

                // the client has issued a recv() call
                case EpollType::ClientRecv:             client_recv_event(data->client);            break;

                // we are bound and have a connection to accept
                case EpollType::ClientListener:         client_listener_event(data->client);        break;

                // an inproc client has received a message
                case EpollType::IntraProcessClientRecv: intraprocess_recv_event(data->inproc);      break;

                // we have received a message from a peer
                case EpollType::ClientPeerRecv:
                {
                    if (data->read)
                    {
                        // resume the read
                        auto read = *data->read;

                        readexact(data->fd, read.message.payload.data + read.cursor, read.message.payload.len - read.cursor, false, 1000);

                        continue;
                    }

                    client_peer_recv_event(data->peer);
                }
            }
            // clang-format on

            // this is something else
            continue;
        }

        if (event.events & EPOLLOUT)
        {
            // write event

            if (data->write)
            {
                // resume the write

                continue;
            }

            // this is an error
            panic("unexpected write event");
        }
    }
}
