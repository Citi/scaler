#include "session.hpp"

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
        return *edata;

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
    peer->write_op = IoOperation::write(peer->client->identity, MessageType::Identity);
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
    }
}

void client_recv_event(Client *client)
{
    // in this event, the recv_event_fd has proc'd
    // but, read the recv_buffer_event_fd first
    // to ensure that a message is available to complete the future

    for (;;)
    {
        if (eventfd_wait(client->recv_buffer_event_fd) < 0)
        {
            if (errno == EAGAIN)
            {
                std::cout << "client_recv_event(): no buffered messages" << std::endl;

                break;
            }

            panic("handle eventfd read error: " + std::to_string(errno));
        }

        // decrement the semaphore
        if (eventfd_wait(client->recv_event_fd) < 0)
        {
            // this should never happen because the epoll proc'd and there's no one to race with us
            if (errno == EAGAIN)
            {
                // we need to re-increment the semaphore because we didn't process the message
                if (eventfd_signal(client->recv_buffer_event_fd) < 0)
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
        switch (*peer->read_op->type)
        {
        case MessageType::Data:
            panic("bad message type while reading identity: remove this after debugging");
            // std::cout << "bad message type while reading identity: data" << std::endl;
            // reconnect_peer(peer);
            // return false;
        case MessageType::Disconnect:
            remove_peer(peer);
            return false; // explicit disconnect
        case MessageType::Identity:
            break; // fall through
        default:
            panic("unknown message type: " + std::to_string((uint8_t)*peer->read_op->type));
        }

        peer->identity = peer->read_op->payload; // set identity
        peer->read_op->complete();               // complete the op
        peer->read_op = std::nullopt;            // reset

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
            .identity = Bytes::empty(),
            .addr = addr,
            .type = PeerType::Connectee,
            .fd = fd,
            .queue = std::queue<SendMessage>(),
            // connecting state
            .state = PeerState::Connecting,
            // we need to read the peer's identity
            .read_op = IoOperation::read(),
            // we need to write our identity
            .write_op = IoOperation::write(client->identity, MessageType::Identity),
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

        // we're edge triggered so it's important that we check this
        client_peer_event_connected(event);
    }
}

void client_peer_event_connected(epoll_event *event)
{
    std::cout << "client_peer_event_connected()" << std::endl;

    auto edata = (EpollData *)event->data.ptr;
    auto peer = edata->peer;

    if (event->events & EPOLLOUT)
        if (epollout_peer(peer) == ControlFlow::Break)
            return;

    if (event->events & EPOLLIN)
    {
        std::cout << "client_peer_event_connected(): reading" << std::endl;

        for (;;)
        {
            if (!peer->read_op)
                peer->read_op = IoOperation::read();

            auto result = read_message(peer->fd, &*peer->read_op);

            switch (result)
            {
            case ReadResult::Blocked2:
                std::cout << "client_peer_event_connected(): read blocked; n: " << peer->read_op->cursor << std::endl;
                return; // return from fn, note: no way to break loop
            case ReadResult::Disconnect2:
            case ReadResult::BadMagic:
                std::cout << "client_peer_event_connected(): disconnect" << std::endl;
                reconnect_peer(peer);
                return;
            case ReadResult::Read:
                std::cout << "client_peer_event_connected(): read message" << std::endl;
                peer->read_op->complete();

                switch (*peer->read_op->type)
                {
                case MessageType::Data:
                    peer->recv_msg(peer->read_op->payload);
                    break;
                case MessageType::Identity:
                    std::cout << "client_peer_event_connected(): unexpected identity message" << std::endl;
                    reconnect_peer(peer);
                    return;
                case MessageType::Disconnect:
                    std::cout << "client_peer_event_connected(): disconnect message!!!" << std::endl;

                    // todo delete peer
                    remove_peer(peer);
                    return; // exit fn
                }

                // reset the read operation
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

    // if (event->events & EPOLLHUP)
    // {
    //     std::cout << "client_peer_event(): unexpected hangup" << std::endl;

    //     reconnect_peer(peer);
    //     return;
    // }

    if (event->events & EPOLLERR)
    {
        int result;
        socklen_t result_len = sizeof(result);
        if (getsockopt(peer->fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
            panic("failed to getsockopt: " + std::to_string(errno));

        if (result == ECONNREFUSED)
            std::cout << "client_peer_event(): connection refused" << std::endl;
        else
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
            request.complete();
            break;
        case ControlOperation::DestroyClient:
        {
            auto client = request.client;
            auto peers = &client->peers;

            // this semaphore will be completed
            // once all the peers have disconnected
            // or the timeout has been reached <- TODO
            // and the client has been destroyed
            request.client->destroy = request.completer;

            for (size_t i = 0; i < peers->size(); i++)
            {
                SendMessage send{
                    .completer = Completer::none(),
                    .msg = {
                        .type = MessageType::Disconnect,
                        .address = Bytes::empty(),
                        .payload = Bytes::empty(),
                    }};

                write_to_peer(peers->at(i), send);
            }
        }

        break;
        case ControlOperation::Connect:
            client_connect_peer(request.peer);
            request.complete();
            break;
        }
    }
}

// either recv() was called or a message was buffered -- same handler
void intraprocess_recv_event(IntraProcessClient *client)
{
    if (eventfd_wait(client->recv_buffer_event_fd) < 0)
    {
        if (errno == EAGAIN)
            return;

        panic("failed to read from eventfd: " + std::to_string(errno));
    }

    if (eventfd_wait(client->recv_event_fd) < 0)
    {
        if (errno == EAGAIN)
        {
            // we need to re-increment the semaphore because we didn't process the message
            if (eventfd_signal(client->recv_buffer_event_fd) < 0)
                panic("failed to write to eventfd: " + std::to_string(errno));

            return;
        }

        panic("failed to read from eventfd: " + std::to_string(errno));
    }

    Message message;
    while (client->queue.try_dequeue(message))
        ; // wait

    void *future;
    while (client->recv.try_dequeue(future))
        ; // wait

    // this is the address of a stack variable
    // ok because the called code copies the message immediately
    future_set_result(future, &message);
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
