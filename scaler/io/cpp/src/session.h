#ifndef SESSION_H_
#define SESSION_H_

// C++
#include <atomic>
#include <shared_mutex>
#include <vector>
#include <queue>
#include <thread>

// System
#include <sys/epoll.h>
#include <sys/timerfd.h>

// --- declarations ---

struct EpollType;
struct WriteOperation;
struct ReadOperation;
struct EpollData;
ENUM ControlOperation : uint8_t;
struct ControlRequest;
struct ThreadContext;
struct Session;

// First-party
#include "common.h"
#include "client.h"
#include "intra_process.h"

void set_sock_opts(int fd);
void accept_peer(ThreadContext *ctx, Peer *peer);
void client_connect_peer(ThreadContext *ctx, Peer *peer);

void resume_read(EpollData *data);
void resume_write(EpollData *data);

// epoll handlers
void client_send_event(Client *client);
void client_recv_event(Client *client);
void client_listener_event(Client *client);
void intraprocess_recv_event(IntraProcessClient *client);

void io_thread_main(ThreadContext *ctx);

void session_init(Session *session, size_t num_threads);


// --- structs ---

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

    std::string as_string() const;

private:
    Value value;
};

// this is an in-progress write operation
// created only after the entire header has been written
struct WriteOperation
{
    void *future;
    Bytes payload;
    size_t cursor;
};

// an in-progress read operation
// created only after the entire header has been read
struct ReadOperation
{
    Message message;
    size_t cursor;
};

struct EpollData
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

ENUM ControlOperation: uint8_t
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
    std::vector<EpollData *> io_cache;
    std::queue<Peer *> connecting;
    ConcurrentQueue<ControlRequest> control;
    int control_efd;
    int epoll_fd;
    int connect_timer_tfd;
    bool timer_armed;

    void arm_timer();

    void accept_peer(Peer *peer);

    void add_client(Client *client);

    void remove_client(Client *client);

    void remove_peer(Peer *peer);

    // must be called on io-thread
    void add_epoll(int fd, uint32_t flags, EpollType type, void *data);

    // must be called on io-thread
    void remove_epoll(int fd);

    EpollData *epoll_by_fd(int fd);

    void save_write(Peer *peer, WriteOperation op);
    void save_read(Peer *peer, ReadOperation op);

    void start();
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

#endif
#if INCLUDE_DEFS

// --- functions ---

std::string EpollType::as_string() const
{
    switch (value)
    {
    case EpollType::ClientSend:
        return "ClientSend";
    case EpollType::ClientRecv:
        return "ClientRecv";
    case EpollType::ClientListener:
        return "ClientListener";
    case EpollType::ClientPeerRecv:
        return "ClientPeerRecv";
    case EpollType::IntraProcessClientRecv:
        return "IntraProcessClientRecv";
    case EpollType::PeerConnecting:
        return "PeerConnecting";
    case EpollType::ConnectTimer:
        return "ConnectTimer";
    case EpollType::Control:
        return "Control";
    case EpollType::Closed:
        return "Closed";
    }
}

void ThreadContext::arm_timer()
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

void ThreadContext::add_client(Client *client)
{
    this->add_epoll(client->send_event_fd, EPOLLIN | EPOLLET, EpollType::ClientSend, client);
    this->add_epoll(client->recv_event_fd, EPOLLIN | EPOLLET, EpollType::ClientRecv, client);
}

void ThreadContext::remove_client(Client *client)
{
    this->remove_epoll(client->send_event_fd);
    this->remove_epoll(client->recv_event_fd);
}

void ThreadContext::remove_peer(Peer *peer)
{
    this->remove_epoll(peer->fd);
}

// must be called on io-thread
void ThreadContext::add_epoll(int fd, uint32_t flags, EpollType type, void *data)
{
    auto edata = new EpollData{
        .fd = fd,
        .type = type,
        .read = std::nullopt,
        .write = std::nullopt,
        .ptr = data,
    };

    epoll_event event{
        .events = flags,
        .data = {.ptr = edata}};

    epoll_ctl(this->epoll_fd, EPOLL_CTL_ADD, fd, &event);

    this->io_cache.push_back(edata);
}

// must be called on io-thread
void ThreadContext::remove_epoll(int fd)
{
    if (epoll_ctl(this->epoll_fd, EPOLL_CTL_DEL, fd, nullptr) < 0)
    {
        // we ignore enoent because it means the fd was already removed
        if (errno != ENOENT)
            panic("failed to remove epoll fd: " + std::to_string(fd) + "; " + strerror(errno));
    }

    auto edata = std::find_if(this->io_cache.begin(), this->io_cache.end(), [fd](EpollData *d)
                              { return d->fd == fd; });

    if (edata != this->io_cache.end())
    {
        delete *edata;
        this->io_cache.erase(edata);
    }
}

EpollData *ThreadContext::epoll_by_fd(int fd)
{
    auto edata = std::find_if(this->io_cache.begin(), this->io_cache.end(), [fd](EpollData *d)
                              { return d->fd == fd; });

    if (edata != this->io_cache.end())
    {
        return *edata;
    }

    return nullptr;
}

void ThreadContext::save_write(Peer *peer, WriteOperation op)
{
    auto edata = this->epoll_by_fd(peer->fd);

    if (!edata)
        panic("failed to find epoll data for peer");

    if (edata->write)
        panic("write operation already in progress");

    edata->write = op;
}

void ThreadContext::save_read(Peer *peer, ReadOperation op)
{
    auto edata = this->epoll_by_fd(peer->fd);

    if (!edata)
        panic("failed to find epoll data for peer");

    if (edata->read)
        panic("read operation already in progress");

    edata->read = op;
}

void ThreadContext::start()
{
    this->thread = std::thread(io_thread_main, this);
}

void set_sock_opts(int fd)
{
    timeval tv{
        .tv_sec = 1,
        .tv_usec = 0};

    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
}

// complete acceptance of peer
// - add it to client's peer list
// - register with thread ctx
void accept_peer(ThreadContext *ctx, Peer *peer)
{
    auto was_muted = peer->client->muted();
    peer->client->peers.push_back(peer);
    ctx->accept_peer(peer);

    if (was_muted)
    {
        peer->client->unmute();
    }
}

// begin the connection process for a peer
// the peer's fd will be overwritten with the new socket
void client_connect_peer(ThreadContext *ctx, Peer *peer)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);

    if (fd < 0)
    {
        panic("failed to create socket");
    }

    peer->fd = fd;
    set_sock_opts(peer->fd);
    auto res = connect(peer->fd, (sockaddr *)&peer->addr, sizeof(peer->addr));

    if (res < 0)
    {
        if (errno == EINPROGRESS)
        {
            // we need to wait for the socket to become writable
            ctx->add_epoll(peer->fd, EPOLLOUT, EpollType::PeerConnecting, peer);
        }

        // todo: handle other errors?
        close(fd);
        return;
    }

    if (res == 0)
    {
        // the socket connected immediately
        accept_peer(ctx, peer);
    }
}

// epoll handlers
void client_send_event(Client *client)
{
    for (;;)
    {
        if (client->muted())
        {
            // zero the semaphore
            eventfd_reset(client->unmuted_event_fd);
            break;
        }

        // decrement the semaphore
        if (eventfd_wait(client->send_event_fd) < 0)
        {
            // semaphore is zero, we can epoll_wait() again
            if (errno == EAGAIN)
                break;

            panic("handle eventfd read error: " + std::to_string(errno));
        }

        // invariant: if we decremented the semaphore the queue must have a message
        // we loop because thread synchronization may be delayed
        SendMsg send;
        while (!client->send_queue.try_dequeue(send))
            ; // wait

        client->send(send);

        // resolve the future
        future_set_result(send.future, NULL);
    }
}

void client_recv_event(Client *client)
{
    // in this event, the recv_event_fd has proc'd
    // but, read the recv_buffer_event_fd first
    // to ensure that a message is available to complete the future

    for (;;)
    {
        eventfd_t value;
        if (eventfd_read(client->recv_buffer_event_fd, &value) < 0)
        {
            if (errno == EAGAIN)
                break;

            panic("handle eventfd read error: " + std::to_string(errno));
        }

        // decrement the semaphore
        if (eventfd_read(client->recv_event_fd, &value) < 0)
        {
            // the semaphore is zero, we can epoll_wait again (edge-triggered)
            if (errno == EAGAIN)
            {
                // we need to re-increment the semaphore because we didn't process the message
                if (eventfd_write(client->recv_buffer_event_fd, 1) < 0)
                    panic("failed to write to eventfd: " + std::to_string(errno));

                break;
            }

            // there aren't really any handle-able errors here
            // maybe EINTR if we are interrupted
            panic("handle eventfd read error:" + std::to_string(errno));
        }

        // invariant: if we decrement the semaphore the queue must have a future
        void *future;
        while (!client->recv_queue.try_dequeue(future))
            ; // wait

        Message msg;
        while (!client->recv_buffer.try_dequeue(msg))
            ; // wait

        // this is the address of a stack variable
        // ok because the called code copies the message immediately
        future_set_result(future, &msg);

        // we're done with the message
        message_destroy(msg);
    }
}

void client_listener_event(Client *client)
{
    for (;;)
    {
        sockaddr_storage addr;
        socklen_t addr_len = sizeof(addr);
        auto fd = accept4(client->fd, (sockaddr *)&addr, &addr_len, SOCK_NONBLOCK);

        if (fd < 0)
        {
            // no more connections to accept; back to epoll_wait()
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;

            panic("todo: accept() error handling");
        }

        set_sock_opts(fd);

        // send our identity
        // this is a blocking operation
        if (!write_message(fd, &client->identity, false))
        {
            close(fd);
            continue;
        }

        Bytes identity;
        if (!read_message(fd, &identity, false))
        {
            close(fd);
            continue;
        }

        auto peer = new Peer{
            .client = client,
            .identity = identity,
            .addr = addr,
            .type = PeerType::Connectee,
            .fd = fd};

        accept_peer(client->thread, peer);
    }
}

void resume_read(EpollData *data)
{
    // auto peer = data->peer;
    // auto op = &*data->read;

    // auto n_bytes = readexact
}

void resume_write(EpollData *data)
{
    auto peer = data->peer;
    auto op = &*data->write;

    auto n_bytes = writeall(peer->fd, op->payload.data + op->cursor, op->payload.len - op->cursor, true);

    // disconnect!
    if (!n_bytes)
        reconnect_peer(peer);
    else
    {
        op->cursor += *n_bytes;

        // if we're done, clear the write operation
        if (op->cursor == op->payload.len)
            data->write = std::nullopt;
    }
}

void intraprocess_recv_event(IntraProcessClient *client)
{
    // TODO
}

void io_thread_main(ThreadContext *ctx)
{
    epoll_event event;
    for (;;)
    {
        auto n_events = epoll_wait(ctx->epoll_fd, &event, 1, -1);

        // spurrious wakeup
        if (n_events == 0)
            continue;

        EpollData *data = (EpollData *)event.data.ptr;

        if (data->type == EpollType::ConnectTimer)
        {
            // the timer is no longer armed
            ctx->timer_armed = false;

            if (timerfd_read2(ctx->connect_timer_tfd) < 0)
            {
                if (errno == EAGAIN)
                    continue;

                panic("failed to read from connect timer: " + std::to_string(errno));
            }

            while (!ctx->connecting.empty())
            {
                auto peer = ctx->connecting.front();
                ctx->connecting.pop();

                client_connect_peer(ctx, peer);
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

                client_connect_peer(ctx, peer);
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
                accept_peer(ctx, data->peer);
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
                        resume_read(data);
                        continue;
                    }

                    client_peer_recv_event(data->peer);
                }
            }
            // clang-format on

            panic("epoll: unexpected read event");
        }

        if (event.events & EPOLLOUT)
        {
            if (data->write)
            {
                resume_write(data);
                continue;
            }

            // this is an error
            panic("unexpected write event");
        }
    }
}

// --- public api ---

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
            .io_cache = std::vector<EpollData *>(),
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

#endif
