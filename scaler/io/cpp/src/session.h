#ifndef SESSION_H
#define SESSION_H

// C++
#include <atomic>
#include <shared_mutex>
#include <vector>
#include <queue>
#include <thread>

// System
#include <sys/epoll.h>
#include <sys/timerfd.h>

// Common
#include "common.h"

// --- declarations ---

struct EpollType;
struct EpollData;
ENUM ControlOperation : uint8_t;
struct ControlRequest;
struct ThreadContext;
struct Session;

// First-party
#include "client.h"
#include "intra_process.h"

void set_sock_opts(int fd);
void accept_peer(Peer *peer);
void client_connect_peer(Peer *peer);

std::optional<ReadOperation> exchange_identity(Client *client, int fd);

void peer_read(Peer *peer);
void resume_write(Peer *peer);

// epoll handlers
void client_send_event(Client *client);
void client_recv_event(Client *client);
void client_listener_event(Client *client);
void client_peer_recv_event(Peer *peer);
void intraprocess_recv_event(IntraProcessClient *client);

void io_thread_main(ThreadContext *ctx);

void session_init(Session *session, size_t num_threads);
void session_destroy(Session *session);

// --- structs ---

struct EpollType
{
    enum Value
    {
        ClientSend,
        ClientRecv,
        ClientListener,
        ClientPeer,
        IntraProcessClientRecv,

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

struct EpollData
{
    int fd;
    EpollType type;

    union
    {
        void *ptr;
        Client *client;
        IntraProcessClient *inproc;
        Peer *peer;
    };
};

ENUM ControlOperation : uint8_t{
                            AddClient,
                            RemoveClient,
                            Connect,
                        };

struct ControlRequest
{
    ControlOperation op;
    std::optional<std::binary_semaphore *> sem;
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
    std::vector<Peer *> connecting;
    ConcurrentQueue<ControlRequest> control_queue;
    int control_efd;
    int epoll_fd;
    int connect_timer_tfd;
    bool timer_armed;

    void arm_timer();
    void add_client(Client *client);
    void add_peer(Peer *peer);
    void remove_client(Client *client);
    void remove_peer(Peer *peer);

    // must be called on io-thread
    void add_epoll(int fd, uint32_t flags, EpollType type, void *data);

    // must be called on io-thread
    void remove_epoll(int fd);

    EpollData *epoll_by_fd(int fd);

    void control(ControlRequest request);

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

    ThreadContext *next_thread()
    {
        auto rr = thread_rr++;
        return &threads[rr % num_threads()];
    }

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
    case EpollType::ClientPeer:
        return "ClientPeer";
    case EpollType::IntraProcessClientRecv:
        return "IntraProcessClientRecv";
    case EpollType::ConnectTimer:
        return "ConnectTimer";
    case EpollType::Control:
        return "Control";
    case EpollType::Closed:
        return "Closed";
    }

    panic("unreachable");
}

void ThreadContext::control(ControlRequest request)
{
    this->control_queue.enqueue(request);

    if (eventfd_signal(this->control_efd) < 0)
        panic("failed to write to eventfd: " + std::to_string(errno));
}

void ThreadContext::arm_timer()
{
    std::cout << "thread[" << this->id << "]: arming timer" << std::endl;

    itimerspec spec{
        .it_interval = {.tv_sec = 0, .tv_nsec = 0},
        .it_value = {.tv_sec = 3, .tv_nsec = 0},
    };

    if (timerfd_settime(this->connect_timer_tfd, 0, &spec, NULL) < 0)
        panic("failed to arm timer");

    this->timer_armed = true;
}

void ThreadContext::add_client(Client *client)
{
    this->add_epoll(client->send_event_fd, EPOLLIN | EPOLLET, EpollType::ClientSend, client);
    this->add_epoll(client->recv_event_fd, EPOLLIN | EPOLLET, EpollType::ClientRecv, client);
}

void ThreadContext::add_peer(Peer *peer)
{
    this->add_epoll(peer->fd, EPOLLIN | EPOLLOUT | EPOLLET, EpollType::ClientPeer, peer);
}

void ThreadContext::remove_client(Client *client)
{
    this->remove_epoll(client->send_event_fd);
    this->remove_epoll(client->recv_event_fd);
}

void ThreadContext::remove_peer(Peer *peer)
{
    this->remove_epoll(peer->fd);

    std::erase_if(this->connecting, [peer](Peer *p)
                  { return p == peer; });
}

// must be called on io-thread
void ThreadContext::add_epoll(int fd, uint32_t flags, EpollType type, void *data)
{
    auto edata = new EpollData{
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
void ThreadContext::remove_epoll(int fd)
{
    if (epoll_ctl(this->epoll_fd, EPOLL_CTL_DEL, fd, NULL) < 0)
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

// void ThreadContext::accept_peer(Peer *peer)
// {
//     this->add_epoll(peer->fd, EPOLLIN | EPOLLET, EpollType::ClientPeerRecv, peer);
// }

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

// void ThreadContext::save_write(Peer *peer, WriteOperation op)
// {
//     auto edata = this->epoll_by_fd(peer->fd);

//     if (!edata)
//         panic("failed to find epoll data for peer");

//     if (edata->write)
//         panic("write operation already in progress");

//     edata->write = op;
// }

// void ThreadContext::save_read(Peer *peer, ReadOperation op)
// {
//     auto edata = this->epoll_by_fd(peer->fd);

//     if (!edata)
//         panic("failed to find epoll data for peer");

//     if (edata->read)
//         panic("read operation already in progress");

//     edata->read = op;
// }

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
void accept_peer(Peer *peer)
{
    auto was_muted = peer->client->muted();
    peer->client->peers.push_back(peer);

    std::cout << "client: " << peer->client->identity.as_string() << ": connected peer: " << peer->identity.as_string() << std::endl;

    if (was_muted)
        peer->client->unmute();
}

// begin the connection process for a peer
// the peer's fd will be overwritten with the new socket
void client_connect_peer(Peer *peer)
{
    peer->fd = socket(peer->client->transport == Transport::TCP ? AF_INET : AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);

    if (peer->fd < 0)
    {
        panic("failed to create socket: " + std::to_string(errno));
    }

    set_sock_opts(peer->fd);

    auto res = connect(peer->fd, (sockaddr *)&peer->addr, sizeof(peer->addr));

    if (res == 0)
    {
        // theory: this doesn't happen on Linux
        panic("connect() returned immediately");
    }

    if (res < 0 && errno != EINPROGRESS)
    {
        // todo: handle other errors?
        panic("failed to connect to peer: " + std::to_string(errno));
    }

    peer->state = PeerState::Connecting;
    peer->client->thread->add_peer(peer);
}

// epoll handlers
void client_send_event(Client *client)
{
    for (;;)
    {
        if (client->muted())
        {
            // zero the semaphore
            if (eventfd_reset(client->unmuted_event_fd) < 0)
                panic("failed to reset eventfd: " + std::to_string(errno));

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
        SendMessage send;
        while (!client->send_queue.try_dequeue(send))
            ; // wait

        client->send(send);

        // resolve the completer
        send.completer.complete(NULL);
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

std::optional<ReadOperation> exchange_identity(Client *client, int fd)
{
    if (!write_message(fd, &client->identity, false))
    {
        std::cout << "disconnect while writing identity" << std::endl;
        return std::nullopt;
    }

    ReadOperation op{
        .progress = ReadProgress::Magic,
        .cursor = 0,
        .buffer = {0},
    };

    if (read_message(fd, op) != ReadMessage::Read)
    {
        std::cout << "disconnect while reading identity" << std::endl;
        return std::nullopt;
    }

    return op;
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

            panic("todo: accept() error handling: " + std::to_string(errno));
        }

        set_sock_opts(fd);

        std::cout << "client: " << client->identity.as_string() << ": accepting connection: " << std::to_string(fd) << std::endl;

        auto result = exchange_identity(client, fd);

        if (!result)
        {
            std::cout << "client_listener_event(): disconnect while exchanging identity" << std::endl;

            close(fd);
            continue;
        }

        // it may be possible to accept the peer immediately
        if (result->completed())
        {
            auto peer = new Peer{
                .client = client,
                .identity = result->payload,
                .addr = addr,
                .type = PeerType::Connectee,
                .fd = fd,
                .state = PeerState::Connected,
                .read_op = std::nullopt,
                .write_op = std::nullopt,
            };

            client->thread->add_peer(peer);
            accept_peer(peer);
        }
        else
        {
            auto peer = new Peer{
                .client = client,
                .identity = {
                    .data = nullptr,
                    .len = 0},
                .addr = addr,
                .type = PeerType::Connectee,
                .fd = fd,
                .state = PeerState::Connecting,
                .read_op = *result,
                .write_op = std::nullopt,
            };

            // identity exchange is in progress
            client->thread->add_peer(peer);
        }
    }
}

void client_peer_event_connecting(epoll_event *event)
{
    auto edata = (EpollData *)event->data.ptr;
    auto peer = edata->peer;

    if (event->events & EPOLLERR || event->events & EPOLLHUP)
    {
        std::cout << "client_peer_event_connecting(): unexpected disconnect" << std::endl;

        reconnect_peer(peer);
        return;
    }

    // this means that the socket has connected
    // start exchanging the identity
    if (event->events & EPOLLOUT)
    {
        auto result = exchange_identity(peer->client, peer->fd);

        if (!result)
        {
            std::cout << "disconnect while exchanging identity (596): " << std::to_string(peer->fd) << std::endl;
            reconnect_peer(peer);
            return;
        }

        if (result->completed())
        {
            peer->identity = result->payload;
            peer->state = PeerState::Connected;
        }
        else
        {
            // if we didn't complete, save it and wait for the fd to become readable
            peer->read_op = *result;
        }
    }

    // we're connecting, so we should only be reading
    // this is the first read, so we need to read the identity
    if (event->events & EPOLLIN)
    {
        if (!peer->read_op)
            panic("client_peer_event_connecting(): no read operation");

        auto &op = *peer->read_op;
        auto result = read_message(peer->fd, op);

        if (result != ReadMessage::Read)
        {
            std::cout << "disconnect while resuming read" << std::endl;
            reconnect_peer(peer);
            return;
        }

        // set the identity and update the peer state
        // also clear the read operation
        if (op.completed())
        {
            peer->identity = op.payload;
            peer->state = PeerState::Connected;

            peer->read_op = std::nullopt;
        }
    }
}

void client_peer_event_connected(epoll_event *event)
{
    auto edata = (EpollData *)event->data.ptr;
    auto peer = edata->peer;

    if (event->events & EPOLLERR || event->events & EPOLLHUP)
    {
        std::cout << "client_peer_event_connected(): unexpected disconnect" << std::endl;

        reconnect_peer(peer);
        return;
    }

    if (event->events & EPOLLOUT)
        resume_write(peer);

    if (event->events & EPOLLIN)
        peer_read(peer);
}

void client_peer_event(epoll_event *event)
{
    auto edata = (EpollData *)event->data.ptr;
    auto peer = edata->peer;

    if (peer->state == PeerState::Disconnected)
        panic("client_peer_event(): peer disconnected");

    if (peer->state == PeerState::Connecting)
        client_peer_event_connecting(event);
    else if (peer->state == PeerState::Connected)
        client_peer_event_connected(event);
}

void peer_read(Peer *peer)
{
    for (;;)
    {
        if (!peer->read_op)
            peer->read_op = ReadOperation{
                .progress = ReadProgress::Magic,
                .cursor = 0,
                .buffer = {0},
            };

        auto op = &*peer->read_op;

        auto result = readexact(peer->fd, op->payload.data + op->cursor, op->payload.len - op->cursor, ReadConfig::SoftBlock, 2000);

        if (result.tag == ReadResult::Disconnect || result.tag == ReadResult::Timeout)
        {
            std::cout << "disconnect while resuming read" << std::endl;
            reconnect_peer(peer);
        }
        else if (result.tag == ReadResult::Read)
        {
            op->cursor += result.n_bytes;

            // if we're done, clear the read operation
            if (op->completed())
            {
                Message message{
                    .address = peer->identity,
                    .payload = op->payload,
                };

                peer->client->recv_msg(std::move(message));

                // reset the read operation
                peer->read_op = std::nullopt;

                // continue loop
            }
        }
        else if (result.tag == ReadResult::NoData)
            break;
    }
}

void resume_write(Peer *peer)
{
    if (!peer->write_op)
        panic("resume_write(): no write operation");

    auto op = &*peer->write_op;

    auto n_bytes = writeall(peer->fd, op->payload.data + op->cursor, op->payload.len - op->cursor, true);

    // disconnect!
    if (!n_bytes)
    {
        std::cout << "disconnect while resuming write" << std::endl;
        reconnect_peer(peer);
    }
    else
    {
        op->cursor += *n_bytes;

        // if we're done, clear the write operation
        if (op->cursor == op->payload.len)
            peer->write_op = std::nullopt;
    }
}

void connect_timer_event(Peer *peer)
{
    auto ctx = peer->client->thread;

    std::cout << "thread[" << ctx->id << "]: connect timer" << std::endl;

    // the timer is no longer armed
    ctx->timer_armed = false;

    if (timerfd_read2(ctx->connect_timer_tfd) < 0)
    {
        if (errno == EAGAIN)
            return;

        panic("failed to read from connect timer: " + std::to_string(errno));
    }

    while (!ctx->connecting.empty())
    {
        auto peer = ctx->connecting.back();
        ctx->connecting.pop_back();

        client_connect_peer(peer);
    }
}

void control_event(ThreadContext *ctx)
{
    for (;;)
    {
        if (eventfd_wait(ctx->control_efd) < 0)
        {
            if (errno == EAGAIN)
                break;

            panic("failed to read from control eventfd: " + std::to_string(errno));
        }

        // a control request has been received
        ControlRequest request;
        while (!ctx->control_queue.try_dequeue(request))
            ; // wait

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
                .client = request.client,
                .identity = {
                    .data = nullptr,
                    .len = 0,
                },
                .addr = *request.addr,
                .type = PeerType::Connector,
                .fd = -1, // a real fd will be assigned later
                .state = PeerState::Disconnected,
                .read_op = std::nullopt,
                .write_op = std::nullopt,
            };

            client_connect_peer(peer);
        }
        break;
        }

        if (request.sem)
            (*request.sem)->release();

        continue;
    }
}

void intraprocess_recv_event(IntraProcessClient *client)
{
    panic("intraprocess_recv_event(): not implemented: " + client->identity.as_string());
}

void io_thread_main(ThreadContext *ctx)
{
    epoll_event event;
    for (;;)
    {
        std::cout << "thread[" << ctx->id << "]: epoll_wait()" << std::endl;

        auto n_events = epoll_wait(ctx->epoll_fd, &event, 1, -1);

        // spurrious wakeup
        if (n_events == 0)
        {
            std::cout << "thread[" << ctx->id << "]: spurrious wakeup" << std::endl;
            continue;
        }

        EpollData *data = (EpollData *)event.data.ptr;

        std::cout << "thread[" << ctx->id << "]: event: " << data->type.as_string() << std::endl;

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
            case EpollType::ClientPeer:             client_peer_event(&event);                  break;
            case EpollType::ConnectTimer:           connect_timer_event(data->peer);            break;
            case EpollType::Control:                control_event(ctx);                         break;
            case EpollType::Closed: { return; }

            default:
            panic("epoll: unexpected read event: " + data->type.as_string());
        }
        // clang-format on
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
        .thread_rr = 0};

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
            .connecting = std::vector<Peer *>(),
            .control_queue = ConcurrentQueue<ControlRequest>(),
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

// this must be called after all registered clients have been destroyed
void session_destroy(Session *session)
{
    if (eventfd_signal(session->epoll_close_efd) < 0)
        panic("failed to write to eventfd: " + std::to_string(errno));

    for (auto &ctx : session->threads)
        ctx.thread.join();

    // run destructor in-place without freeing memory (it's owned by Python)
    session->~Session();
}

#endif
