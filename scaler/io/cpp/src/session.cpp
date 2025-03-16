#include "session.hpp"

std::string EpollType::as_string() const
{
    switch (value)
    {
    case EpollType::ConnectorSend:
        return "ConnectorSend";
    case EpollType::ConnectorRecv:
        return "ConnectorRecv";
    case EpollType::ConnectorListener:
        return "ConnectorListener";
    case EpollType::ConnectorPeer:
        return "ConnectorPeer";
    case EpollType::ConnectorDestroyTimeout:
        return "ConnectorDestroyTimeout";
    case EpollType::IntraProcessConnectorRecv:
        return "IntraProcessConnectorRecv";
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

// arm the reconnect timer if it is not already armed
void ThreadContext::ensure_timer_armed()
{
    if (this->timer_armed)
        return;

    itimerspec spec{
        .it_interval = {.tv_sec = 0, .tv_nsec = 0},
        .it_value = {.tv_sec = 3, .tv_nsec = 0},
    };

    if (timerfd_settime(this->connect_timer_tfd, 0, &spec, NULL) < 0)
        panic("failed to arm timer");

    this->timer_armed = true;
}

void ThreadContext::add_connector(NetworkConnector *connector)
{
    this->add_epoll(connector->send_event_fd, EPOLLIN | EPOLLET, EpollType::ConnectorSend, connector);
    this->add_epoll(connector->recv_event_fd, EPOLLIN | EPOLLET, EpollType::ConnectorRecv, connector);
}

void ThreadContext::add_peer(RawPeer *peer)
{
    this->add_epoll(peer->fd, EPOLLIN | EPOLLOUT | EPOLLET, EpollType::ConnectorPeer, peer);
}

void ThreadContext::remove_client(NetworkConnector *connector)
{
    this->remove_epoll(connector->send_event_fd);
    this->remove_epoll(connector->recv_event_fd);
}

// this free's the peer's EpollData
void ThreadContext::remove_peer(RawPeer *peer)
{
    this->remove_epoll(peer->fd);

    std::erase_if(this->connecting, [peer](RawPeer *p)
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
    // std::cout << "removing epoll fd: " << std::to_string(fd) << std::endl;

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
void complete_peer_connect(RawPeer *peer)
{
    peer->state = PeerState::Connected;

    auto was_muted = peer->connector->muted();
    peer->connector->peers.push_back(peer);

    if (was_muted)
        peer->connector->unmute();
}

void network_connector_connect_peer_inner(RawPeer *peer, int domain, socklen_t len)
{
    peer->fd = socket(domain, SOCK_STREAM | SOCK_NONBLOCK, 0);

    if (peer->fd < 0)
        panic("failed to create socket: " + std::to_string(errno));

    auto res = connect(peer->fd, (sockaddr *)&peer->addr, len);

    if (res < 0 && errno != EINPROGRESS)
        panic("failed to connect to peer: " + std::to_string(errno));

    peer->state = PeerState::Connecting; // set peer state
    peer->write_op = IoOperation::write(
        peer->connector->identity,
        MessageType::Identity);          // write our identity
    peer->read_op = IoOperation::read(); // read the peer's identity

    peer->connector->thread->add_peer(peer);
}

// begin the connection process for a peer
// the peer's fd will be overwritten with the new socket
void network_connector_connect_peer(RawPeer *peer)
{
    switch (peer->connector->transport)
    {
    case Transport::TCP:
        return network_connector_connect_peer_inner(peer, AF_INET, sizeof(sockaddr_in));
    case Transport::InterProcess:
        return network_connector_connect_peer_inner(peer, AF_UNIX, sizeof(sockaddr_un));
    case Transport::IntraProcess:
        panic("unsupported");
    }
}

// epoll handlers
void network_connector_send_event(NetworkConnector *connector)
{
    for (;;)
    {
        if (connector->muted())
        {
            // std::cout << "client: " << connector->identity.as_string() << ": muted" << std::endl;

            return;
        }

        if (eventfd_wait(connector->send_event_fd) < 0)
        {
            // semaphore is zero, we can epoll_wait() again
            if (errno == EAGAIN)
                break;

            panic("handle eventfd read error: " + std::to_string(errno));
        }

        // invariant: if we decremented the semaphore the queue must have a message
        // we loop because thread synchronization may be delayed
        SendMessage send;
        while (!connector->send_queue.try_dequeue(send))
            ; // wait

        // std::cout << "client: " << connector->identity.as_string() << ": sending message to: " << send.msg.address.as_string() << std::endl;

        connector->send(send);
    }
}

void network_connector_recv_event(NetworkConnector *connector)
{
    // in this event, the recv_event_fd has proc'd
    // but, read the recv_buffer_event_fd first
    // to ensure that a message is available to complete the future

    for (;;)
    {
        if (eventfd_wait(connector->recv_buffer_event_fd) < 0)
        {
            if (errno == EAGAIN)
            {
                // std::cout << "network_connector_recv_event(): no buffered messages" << std::endl;

                break;
            }

            panic("handle eventfd read error: " + std::to_string(errno));
        }

        // decrement the semaphore
        if (eventfd_wait(connector->recv_event_fd) < 0)
        {
            // there are no more recvs
            if (errno == EAGAIN)
            {
                // we decremented the buffer efd but aren't processing a message
                // so we need to re-increment the semaphore
                if (eventfd_signal(connector->recv_buffer_event_fd) < 0)
                    panic("failed to signal eventfd: " + std::to_string(errno));

                break; // back to epoll_wait()
            }

            panic("handle eventfd read error:" + std::to_string(errno));
        }

        // invariant: if we decrement the semaphore the queue must have a future
        void *future;
        while (!connector->recv_queue.try_dequeue(future))
            ; // wait

        Message message;
        while (!connector->recv_buffer.try_dequeue(message))
            ; // wait

        // std::cout << "network_connector_recv_event(): completing future" << std::endl;

        // this is the address of a stack variable
        // ok because the called code copies the message immediately
        future_set_result(future, &message);

        // we're done with the message
        message_destroy(&message);
    }
}

// may call reconnect_peer()
// true -> ok
// false -> disconnected
bool write_identity(RawPeer *peer)
{
    auto result = write_message(peer->fd, &*peer->write_op);

    switch (result)
    {
    case WriteResult::Done:
        peer->write_op->complete();
        peer->write_op = std::nullopt;
        // std::cout << "client: " << peer->connector->identity.as_string() << ": wrote identity to peer: " << peer->identity.as_string() << std::endl;
        [[fallthrough]];
    case WriteResult::Blocked:
        return true;
    case WriteResult::Disconnect:
        // std::cout << "disconnect while writing identity" << std::endl;
        reconnect_peer(peer);
        return false;
    }

    panic("unreachable");
}

// may call reconnect_peer()
// true -> ok
// false -> disconnected
bool read_identity(RawPeer *peer)
{
    auto result = read_message(peer->fd, &*peer->read_op);

    switch (result)
    {
    case ReadResult::Read:
        switch (*peer->read_op->type)
        {
        case MessageType::Data:
            panic("bad message type while reading identity: remove this after debugging");
            // // std::cout << "bad message type while reading identity: data" << std::endl;
            // reconnect_peer(peer);
            // return false;
        case MessageType::Disconnect:
            remove_peer(peer);
            delete peer;
            return false; // explicit disconnect
        case MessageType::Identity:
            break; // fall through
        default:
            panic("unknown message type: " + std::to_string((uint8_t)*peer->read_op->type));
        }

        peer->identity = peer->read_op->payload; // set identity
        peer->read_op->complete();               // complete the op
        peer->read_op = std::nullopt;            // reset

        // std::cout << "client: " << peer->connector->identity.as_string() << ": connected to peer: " << peer->identity.as_string() << std::endl;

        [[fallthrough]];
    case ReadResult::Blocked:
        return true;
    case ReadResult::BadMagic:
        // std::cout << "bad magic while reading identity" << std::endl;
        reconnect_peer(peer);
        return false;
    case ReadResult::BadType:
        // std::cout << "bad type while reading identity" << std::endl;
        reconnect_peer(peer);
        return false;
    case ReadResult::Disconnect:
        // std::cout << "disconnect while reading identity" << std::endl;
        reconnect_peer(peer);
        return false;
    }

    panic("unreachable");
}

void network_connector_listener_event(NetworkConnector *connector)
{
    for (;;)
    {
        sockaddr_storage addr;
        socklen_t addr_len = sizeof(addr);
        auto fd = accept4(connector->fd, (sockaddr *)&addr, &addr_len, SOCK_NONBLOCK);

        if (fd < 0)
        {
            // no more connections to accept; back to epoll_wait()
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;

            panic("todo: accept() error handling: " + std::to_string(errno));
        }

        set_sock_opts(fd);

        // std::cout << "client: " << connector->identity.as_string() << ": accepting connection: " << std::to_string(fd) << std::endl;

        // create the peer and add it to the epoll
        // when it becomes writable the common logic
        // will take care of exchanging the identity
        // and all the other stuff
        auto peer = new RawPeer{
            .connector = connector,
            .identity = Bytes::empty(),
            .addr = addr,
            .type = PeerType::Connectee,
            .fd = fd,
            .queue = std::queue<SendMessage>(),
            .state = PeerState::Connecting,
            // read the peer's identity
            .read_op = IoOperation::read(),
            // write our identity
            .write_op = IoOperation::write(connector->identity, MessageType::Identity),
        };

        connector->thread->add_peer(peer);
    }
}

void network_connector_peer_event_connecting(epoll_event *event)
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
        complete_peer_connect(peer);

        // we're edge triggered so it's important that we check this
        network_connector_peer_event_connected(event);
    }
}

void network_connector_peer_event_connected(epoll_event *event)
{
    // std::cout << "network_connector_peer_event_connected()" << std::endl;

    auto edata = (EpollData *)event->data.ptr;
    auto peer = edata->peer;

    if (event->events & EPOLLOUT)
        if (epollout_peer(peer) == ControlFlow::Break)
            return;

    if (event->events & EPOLLIN)
        if (epollin_peer(peer) == ControlFlow::Break)
            return;
}

// may call reconnect_peer()
void network_connector_peer_event(epoll_event *event)
{
    auto edata = (EpollData *)event->data.ptr;
    auto peer = edata->peer;

    // if (event->events & EPOLLHUP)
    // {
    //     // std::cout << "network_connector_peer_event(): unexpected hangup" << std::endl;

    //     reconnect_peer(peer);
    //     return;
    // }

    if (event->events & EPOLLERR)
    {
        int result;
        socklen_t result_len = sizeof(result);
        if (getsockopt(peer->fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
            panic("failed to getsockopt: " + std::to_string(errno));

        // if (result == ECONNREFUSED)
        // std::cout << "network_connector_peer_event(): connection refused" << std::endl;
        // else
        // std::cout << "network_connector_peer_event(): unexpected error: " << std::to_string(result) << std::endl;

        reconnect_peer(peer);
        return;
    }

    if (peer->state == PeerState::Disconnected)
        panic("network_connector_peer_event(): peer is disconnected");

    if (peer->state == PeerState::Connecting)
        network_connector_peer_event_connecting(event);
    else if (peer->state == PeerState::Connected)
        network_connector_peer_event_connected(event);
}

void network_connector_destroy_timeout(NetworkConnector *connector)
{
    // todo: destroy the client
    // share code with other places client needs to be destroyed:
    //  - control_event(), when there's no peers
    //  - epollout_peer(), when the last peer has disconnected

    // std::cout << "network_connector_destroy_timeout(): client destroy timed out" << std::endl;

    connector->destroy->complete();
}

void connect_timer_event(ThreadContext *ctx)
{
    for (int i = 0;; i++)
    {
        // std::cout << "thread[" << ctx->id << "]: connect timer; " << i << std::endl;

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

            network_connector_connect_peer(peer);
        }
    }

    if (!ctx->connecting.empty() && !ctx->timer_armed)
        ctx->ensure_timer_armed();
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
        case ControlOperation::AddConnector:
            // std::cout << "control_event(): add client" << std::endl;

            ctx->add_connector(request.connector);
            request.complete();
            break;
        case ControlOperation::DestroyConnector:
            // std::cout << "control_event(): destroy" << std::endl;

            {
                auto connector = request.connector;
                auto &peers = connector->peers;

                if (peers.empty())
                    // todo: remove client from thread
                    // share logic with epollout
                    request.complete();
                else
                {
                    // this semaphore will be completed
                    // once all the peers have disconnected
                    // or the timeout has been reached <- TODO
                    // and the client has been destroyed
                    connector->destroy = request.completer;

                    SendMessage send{
                        .completer = Completer::none(),
                        .msg = {
                            .type = MessageType::Disconnect,
                            .address = Bytes::empty(),
                            .payload = Bytes::empty(),
                        }};

                    for (auto peer : peers)
                        write_to_peer(peer, send);

                    auto tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
                    if (tfd < 0)
                        panic("failed to create timerfd: " + std::to_string(errno));

                    itimerspec spec{
                        .it_interval = {
                            .tv_sec = 0,
                            .tv_nsec = 0,
                        },
                        .it_value = {
                            .tv_sec = 5,
                            .tv_nsec = 0,
                        }};

                    if (timerfd_settime(tfd, 0, &spec, NULL) < 0)
                        panic("failed to arm timer: " + std::to_string(errno));

                    connector->destroy_tfd = tfd;
                    ctx->add_epoll(connector->destroy_tfd, EPOLLIN, EpollType::ConnectorDestroyTimeout, connector);
                }
            }

            break;
        case ControlOperation::Connect:
            // std::cout << "control_event(): connect" << std::endl;

            network_connector_connect_peer(request.peer);
            request.complete();
            break;
        }
    }
}

// either recv() was called or a message was buffered -- same handler
void intra_process_recv_event(IntraProcessConnector *connector)
{
    if (eventfd_wait(connector->recv_buffer_event_fd) < 0)
    {
        if (errno == EAGAIN)
            return;

        panic("failed to read from eventfd: " + std::to_string(errno));
    }

    if (eventfd_wait(connector->recv_event_fd) < 0)
    {
        if (errno == EAGAIN)
        {
            // we need to re-increment the semaphore because we didn't process the message
            if (eventfd_signal(connector->recv_buffer_event_fd) < 0)
                panic("failed to write to eventfd: " + std::to_string(errno));

            return;
        }

        panic("failed to read from eventfd: " + std::to_string(errno));
    }

    Message message;
    while (connector->queue.try_dequeue(message))
        ; // wait

    void *future;
    while (connector->recv.try_dequeue(future))
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
        // std::cout << "thread[" << ctx->id << "]: epoll_wait()" << std::endl;

        auto n_events = epoll_wait(ctx->epoll_fd, &event, 1, -1);

        // spurrious wakeup
        if (n_events == 0)
            continue;

        EpollData *data = (EpollData *)event.data.ptr;

        // std::cout << "thread[" << ctx->id << "]: event: " << data->type.as_string() << std::endl;

        // clang-format off
        switch (data->type)
        {
            case EpollType::ConnectorSend:             network_connector_send_event(data->connector);      break;  // client send()            ET  
            case EpollType::ConnectorRecv:             network_connector_recv_event(data->connector);      break;  // client recv()            ET
            case EpollType::ConnectorListener:         network_connector_listener_event(data->connector);  break;  // new connection           ET
            case EpollType::ConnectorDestroyTimeout:   network_connector_destroy_timeout(data->connector); break;  // client destroy timed out LT
            case EpollType::ConnectorPeer:             network_connector_peer_event(&event);               break;  // peer has data            ET
            case EpollType::IntraProcessConnectorRecv: intra_process_recv_event(data->inproc);   break;  // intraprocess recv()      ET
            case EpollType::ConnectTimer:           connect_timer_event(ctx);                break;  // connect timer            LT
            case EpollType::Control:                control_event(ctx);                      break;  // control event            LT
            case EpollType::Closed:                                                          return; // exit                     LT

            default:
                panic("epoll: unknown event type");
        }
        // clang-format on
    }
}

// --- public api ---

void session_init(Session *session, size_t num_threads)
{
    new (session) Session{
        .threads = std::vector<ThreadContext>(),
        .inprocs = std::vector<IntraProcessConnector *>(),
        .intra_process_mutex = std::shared_mutex(),
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
            .connecting = std::vector<RawPeer *>(),
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
