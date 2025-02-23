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
#include "common.hpp"

// --- declarations ---

struct EpollType;
struct EpollData;
ENUM ControlOperation : uint8_t;
struct ControlRequest;
struct ThreadContext;
struct Session;

// First-party
#include "client.hpp"
#include "intra_process.hpp"

void set_sock_opts(int fd);
void complete_peer_connect(Peer *peer);
void client_connect_peer(Peer *peer);

bool read_identity(Peer *peer);
bool write_identity(Peer *peer);

// epoll handlers
void client_send_event(Client *client);
void client_recv_event(Client *client);
void client_listener_event(Client *client);
void client_peer_event_connecting(epoll_event *event);
void client_peer_event_connected(epoll_event *event);
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
    Completer completer;
    std::optional<sockaddr_storage> addr;

    union
    {
        void *data;
        Client *client;
    };

    void complete(void *result = NULL)
    {
        completer.complete(result);
    }
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
    int epoll_close_efd;
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

    panic("unreachable: " + std::to_string(value));
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

// this free's the peer's EpollData
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
    std::cout << "removing epoll fd: " << std::to_string(fd) << std::endl;

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
void complete_peer_connect(Peer *peer)
{
    peer->state = PeerState::Connected;

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

    // set peer states
    peer->state = PeerState::Connecting;
    // write our identity
    peer->write_op = IoOperation::write(peer->client->identity);
    // read the peer's identity
    peer->read_op = IoOperation::read();

    peer->client->thread->add_peer(peer);
}

// epoll handlers
void client_send_event(Client *client)
{
    for (;;)
    {
        if (client->muted())
        {
            std::cout << "client: " << client->identity.as_string() << ": muted" << std::endl;

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

        std::cout << "client: " << client->identity.as_string() << ": sending message to: " << send.msg.address.as_string() << std::endl;

        client->send(send);

        // resolve the completer
        send.completer.complete();
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
            // this should never happen because the epoll proc'd and there's no one to race with us
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

        std::cout << "client_recv_event(): completing future" << std::endl;

        // this is the address of a stack variable
        // ok because the called code copies the message immediately
        future_set_result(future, &msg);

        // we're done with the message
        message_destroy(msg);
    }
}

// may call reconnect_peer()
// true -> ok
// false -> disconnected
bool write_identity(Peer *peer)
{
    auto result = write_message(peer->fd, &*peer->write_op);

    switch (result)
    {
    case WriteResult::Done1:
        peer->write_op->complete();
        peer->write_op = std::nullopt;
        std::cout << "client: " << peer->client->identity.as_string() << ": wrote identity to peer: " << peer->identity.as_string() << std::endl;
        [[fallthrough]];
    case WriteResult::Blocked1:
        return true;
    case WriteResult::Disconnect1:
        std::cout << "disconnect while writing identity" << std::endl;
        reconnect_peer(peer);
        return false;
    }

    panic("unreachable");
}

// may call reconnect_peer()
// true -> ok
// false -> disconnected
bool read_identity(Peer *peer)
{
    auto result = read_message(peer->fd, &*peer->read_op);

    switch (result)
    {
    case ReadResult::Read:
        peer->identity = peer->read_op->payload;
        peer->read_op->complete();
        peer->read_op = std::nullopt;

        std::cout << "client: " << peer->client->identity.as_string() << ": connected to peer: " << peer->identity.as_string() << std::endl;

        [[fallthrough]];
    case ReadResult::Blocked2:
        return true;
    case ReadResult::BadMagic:
        std::cout << "bad magic while reading identity" << std::endl;
        [[fallthrough]];
    case ReadResult::Disconnect2:
        std::cout << "disconnect while reading identity" << std::endl;
        reconnect_peer(peer);
        return false;
    }

    panic("unreachable");
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

        // create the peer and add it to the epoll
        // when it becomes writable the common logic
        // will take care of exchanging the identity
        // and all the other stuff
        auto peer = new Peer{
            .client = client,
            .identity = {
                .data = nullptr,
                .len = 0},
            .addr = addr,
            .type = PeerType::Connectee,
            .fd = fd,
            // connecting state
            .state = PeerState::Connecting,
            // we need to read the peer's identity
            .read_op = IoOperation::read(),
            // we need to write our identity
            .write_op = IoOperation::write(client->identity),
        };

        client->thread->add_peer(peer);
    }
}

void client_peer_event_connecting(epoll_event *event)
{
    auto data = (EpollData *)event->data.ptr;
    auto peer = data->peer;

    if (event->events & EPOLLIN && peer->read_op)
        if (!read_identity(peer))
            return;

    if (event->events & EPOLLOUT && peer->write_op)
        if (!write_identity(peer))
            return;

    // check if we're done
    if (!peer->read_op && !peer->write_op)
    {
        std::cout << "client_peer_event_connecting(): CONNECTED" << std::endl;

        complete_peer_connect(peer);
    }
}

void client_peer_event_connected(epoll_event *event)
{
    std::cout << "client_peer_event_connected()" << std::endl;

    auto edata = (EpollData *)event->data.ptr;
    auto peer = edata->peer;

    if (event->events & EPOLLOUT && peer->write_op)
    {
        std::cout << "client_peer_event_connected(): resuming write" << std::endl;

        if (!peer->write_op)
            panic("resume_write(): no write operation");

        auto result = write_message(peer->fd, &*peer->write_op);

        switch (result)
        {
        case WriteResult::Blocked1:
            break;
        case WriteResult::Disconnect1:
            std::cout << "disconnect while resuming write" << std::endl;
            reconnect_peer(peer);
            return; // we need to go back to epoll_wait() after calling reconnect_peer()
        case WriteResult::Done1:
            peer->write_op->complete();
            peer->write_op = std::nullopt;
        }
    }

    if (event->events & EPOLLIN)
    {
        std::cout << "client_peer_event_connected(): resuming read" << std::endl;

        for (;;)
        {
            if (!peer->read_op)
                peer->read_op = IoOperation::read();

            auto result = read_message(peer->fd, &*peer->read_op);

            switch (result)
            {
            case ReadResult::Blocked2:
                std::cout << "client_peer_event_connected(): blocked" << std::endl;
                return; // return from fn, note: no way to break loop
            case ReadResult::Disconnect2:
            case ReadResult::BadMagic:
                std::cout << "client_peer_event_connected(): disconnect" << std::endl;
                reconnect_peer(peer);
                return;
            case ReadResult::Read:
                std::cout << "client_peer_event_connected(): read message" << std::endl;

                peer->recv_msg(peer->read_op->payload);

                // reset the read operation
                peer->read_op->complete();
                peer->read_op = std::nullopt;
            }
        }
    }
}

// may call reconnect_peer()
void client_peer_event(epoll_event *event)
{
    auto edata = (EpollData *)event->data.ptr;
    auto peer = edata->peer;

    if (event->events & EPOLLHUP)
    {
        std::cout << "client_peer_event(): unexpected hangup" << std::endl;

        reconnect_peer(peer);
        return;
    }

    if (event->events & EPOLLERR)
    {
        int result;
        socklen_t result_len = sizeof(result);
        if (getsockopt(peer->fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
            panic("failed to getsockopt: " + std::to_string(errno));

        std::cout << "client_peer_event(): unexpected error: " << std::to_string(result) << std::endl;

        reconnect_peer(peer);
        return;
    }

    if (peer->state == PeerState::Disconnected)
        panic("client_peer_event(): peer is disconnected");

    if (peer->state == PeerState::Connecting)
        client_peer_event_connecting(event);
    else if (peer->state == PeerState::Connected)
        client_peer_event_connected(event);
}

void connect_timer_event(ThreadContext *ctx)
{
    for (int i = 0;; i++)
    {
        std::cout << "thread[" << ctx->id << "]: connect timer; " << i << std::endl;

        if (timerfd_read2(ctx->connect_timer_tfd) < 0)
        {
            if (errno == EAGAIN)
                break;

            panic("failed to read from connect timer: " + std::to_string(errno));
        }

        if (!ctx->connecting.empty())
        {
            auto peer = ctx->connecting.back();
            ctx->connecting.pop_back();

            client_connect_peer(peer);
        }
    }

    if (!ctx->connecting.empty() && !ctx->timer_armed)
        ctx->arm_timer();
    else
        ctx->timer_armed = false;
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

        request.complete();

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
            case EpollType::ConnectTimer:           connect_timer_event(ctx);                   break;
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
            .epoll_close_efd = eventfd(0, 0),

            .connect_timer_tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK),
            .timer_armed = false,
        });

        auto ctx = &session->threads.back();

        ctx->add_epoll(ctx->epoll_close_efd, EPOLLIN, EpollType::Closed, nullptr);
        ctx->add_epoll(ctx->control_efd, EPOLLIN, EpollType::Control, nullptr);
        ctx->add_epoll(ctx->connect_timer_tfd, EPOLLIN, EpollType::ConnectTimer, nullptr);

        ctx->start();
    }
}

// this must be called after all registered clients have been destroyed
void session_destroy(Session *session)
{
    for (auto &ctx : session->threads)
    {
        if (eventfd_signal(ctx.epoll_close_efd) < 0)
            panic("failed to write to eventfd: " + std::to_string(errno));

        ctx.thread.join();
    }

    // run destructor in-place without freeing memory (it's owned by Python)
    session->~Session();
}

#endif
