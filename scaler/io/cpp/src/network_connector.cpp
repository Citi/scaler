#include "network_connector.hpp"

bool NetworkConnector::peer_by_id(Bytes id, RawPeer **peer)
{
    auto it = std::find_if(this->peers.begin(), this->peers.end(), [id](RawPeer *p)
                           { return p->identity == id; });

    if (it != this->peers.end())
    {
        *peer = *it;
        return true;
    }

    return false;
}

void NetworkConnector::remove_peer(RawPeer *peer)
{
    std::erase(this->peers, peer);
}

bool NetworkConnector::muted()
{
    // these types mute when they have no peers
    if (this->type == ConnectorType::Pair || this->type == ConnectorType::Dealer)
        return this->peers.empty();

    // other types drop messages when they have no peers
    return false;
}

size_t NetworkConnector::peer_rr()
{
    // why modulo twice? the number of peers might have changed
    auto rr = this->rr;
    this->rr = (this->rr + 1) % this->peers.size();

    return rr % this->peers.size();
}

// receive a message
// this will either complete a waiting recv request or buffer the message
void NetworkConnector::recv_msg(Message message)
{
    // if there's a waiting recv, complete it immediately
    if (eventfd_wait(this->recv_event_fd) == 0)
    {

        void *future;
        while (!this->recv_queue.try_dequeue(future))
            ; // wait

        future_set_result(future, &message);
        message_destroy(&message);
    }
    else if (errno == EAGAIN) // o.w. res < 0
    {

        // buffer the message
        this->recv_buffer.enqueue(message);

        if (eventfd_signal(this->recv_buffer_event_fd) < 0)
            panic("failed to write to eventfd: " + std::to_string(errno));
    }
    else
        panic("failed to read eventfd: " + std::to_string(errno));
};

void NetworkConnector::unmute()
{
    // these types do not mute
    if (this->type == ConnectorType::Pub || this->type == ConnectorType::Router)
        return;

    network_connector_send_event(this);
}

// panics if the client is muted
void NetworkConnector::send(SendMessage send)
{
    switch (this->type)
    {
    case ConnectorType::Pair:
    {

        if (this->peers.empty())
            panic("client: muted");

        auto peer = this->peers[0];
        write_to_peer(peer, send);
    }
    break;
    case ConnectorType::Router:
    {

        RawPeer *peer;
        if (!this->peer_by_id(send.msg.address, &peer))
            break; // routers drop messages

        write_to_peer(peer, send);
    }
    break;
    case ConnectorType::Pub:
    {
        // NOTE: IMPLEMENT THIS
        // the completer needs to be completed once the message is written to every peer
        // do we need to use a counting semaphore? maybe we can use an atomic or something?

        // for now just complete the request, it's not essential to the function of the scaler
        send.completer.complete();

        // ---

        // if the socket has no peers, the message is dropped
        // we need to copy the peers because the vector may be modified
        // for (auto peer : std::vector(this->peers))
        //     write_to_peer(peer, send);
    }
    break;
    case ConnectorType::Dealer:
    {

        if (this->peers.empty())
            panic("client: muted");

        // dealers round-robin their peers
        auto peer = this->peers[this->peer_rr()];

        write_to_peer(peer, send);
    }
    break;
    default:
        panic("unknown client type");
    }
}

// takes ownership of the `payload`
void RawPeer::recv_msg(Bytes payload)
{
    Message message{
        .type = MessageType::Data,

        // the lifetime of the identity and this message are decoupled
        // so it's important that we clone the data
        .address = Bytes::clone(this->identity),
        .payload = payload,
    };

    this->connector->recv_msg(message);
}

// try to write `len` bytes of `data` to `fd`
// this is a nonblocking operation and may only write some of the bytes
[[nodiscard]] IoResult writeall(int fd, uint8_t *data, size_t len)
{
    size_t total = 0;

    while (total < len)
    {
        auto n = write(fd, data + total, len - total);

        if (n < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return {
                    .tag = IoResult::Blocked,
                    .n_bytes = total,
                };

            if (errno == EPIPE || errno == ECONNRESET)
                return {
                    .tag = IoResult::Disconnect,
                    .n_bytes = total,
                };

            // todo: handle other errors?
            panic("write error: " + std::to_string(errno));
        }

        total += n;
    }

    return {
        .tag = IoResult::Done,
        .n_bytes = total,
    };
}

// write a message
// nonblocking, resumable
[[nodiscard]] WriteResult write_message(int fd, IoOperation *op)
{
    switch (op->progress)
    {
    case IoProgress::Magic:
    {
        auto result = writeall(fd, MAGIC + op->cursor, 4 - op->cursor);
        op->cursor += result.n_bytes;

        if (result.tag == IoResult::Disconnect)
            return WriteResult::Disconnect;

        if (result.tag == IoResult::Blocked)
            return WriteResult::Blocked;

        op->progress = IoProgress::Type;
        op->cursor = 0;
    }
        [[fallthrough]];
    case IoProgress::Type:
    {
        uint8_t type[] = {(uint8_t)*op->type};

        auto result = writeall(fd, type, 1);

        if (result.tag == IoResult::Disconnect)
            return WriteResult::Disconnect;

        if (result.tag == IoResult::Blocked)
            return WriteResult::Blocked;

        op->progress = IoProgress::Header;
        op->cursor = 0;
    }
        [[fallthrough]];
    case IoProgress::Header:
    {
        // serialize the header
        // this may happen multiple times if we get blocked
        uint8_t header[4];
        serialize_u32(htonl((uint32_t)op->payload.len), header);

        auto result = writeall(fd, header + op->cursor, 4 - op->cursor);
        op->cursor += result.n_bytes;

        if (result.tag == IoResult::Disconnect)
            return WriteResult::Disconnect;

        if (result.tag == IoResult::Blocked)
            return WriteResult::Blocked;

        op->progress = IoProgress::Payload;
        op->cursor = 0;
    }
        [[fallthrough]];
    case IoProgress::Payload:
    {
        auto result = writeall(fd, op->payload.data + op->cursor, op->payload.len - op->cursor);
        op->cursor += result.n_bytes;

        if (result.tag == IoResult::Disconnect)
            return WriteResult::Disconnect;

        if (result.tag == IoResult::Blocked)
            return WriteResult::Blocked;

        return WriteResult::Done;
    }
    }

    panic("unreachable");
}

ControlFlow epollin_peer(RawPeer *peer)
{
    for (;;)
    {
        if (!peer->read_op)
            peer->read_op = IoOperation::read();

        auto result = read_message(peer->fd, &*peer->read_op);

        switch (result)
        {
        case ReadResult::Blocked:

            return ControlFlow::Continue; // return from fn, note: no way to break loop
        case ReadResult::Disconnect:
        case ReadResult::BadMagic:
        case ReadResult::BadType:

            reconnect_peer(peer);
            return ControlFlow::Break;
        case ReadResult::Read:

            peer->read_op->complete();

            switch (*peer->read_op->type)
            {
            case MessageType::Data:
                peer->recv_msg(peer->read_op->payload);

                // reset the read operation
                // but don't free the payload;
                // that's handled when the message is cleaned up
                peer->read_op = std::nullopt;
                break;
            case MessageType::Identity:

                reconnect_peer(peer);
                return ControlFlow::Break;
            case MessageType::Disconnect:

                remove_peer(peer);
                delete peer;
                return ControlFlow::Break; // exit fn
            }
        }
    }
}

// process the send queue until the socket blocks or the queue is exhausted
ControlFlow epollout_peer(RawPeer *peer)
{

    for (;;)
    {
        if (!peer->write_op)
        {
            if (peer->queue.empty())
            {

                return ControlFlow::Continue; // queue exhausted
            }

            auto send = peer->queue.front();
            peer->write_op = IoOperation::write(send.msg.payload, send.msg.type, send.completer);
            peer->queue.pop();
        }

        auto result = write_message(peer->fd, &*peer->write_op);

        switch (result)
        {
        case WriteResult::Blocked:

            return ControlFlow::Continue;
        case WriteResult::Disconnect:

            reconnect_peer(peer);
            return ControlFlow::Break; // we need to go back to epoll_wait() after calling reconnect_peer()
        case WriteResult::Done:

            peer->write_op->complete();

            if (peer->write_op->type == MessageType::Disconnect)
            {
                remove_peer(peer);

                if (peer->connector->destroy && peer->connector->peers.empty())
                {

                    // the client is being destroyed and the last peer has disconnected

                    // TODO!!!!

                    peer->connector->destroy->complete();
                    peer->connector->destroy = std::nullopt;
                }

                delete peer;
                return ControlFlow::Break;
            }

            peer->write_op = std::nullopt;
        }
    }
}

// note: peer may be in reconnecting state after calling this
// the peer's EpollData may have been freed
void write_to_peer(RawPeer *peer, SendMessage send)
{
    peer->queue.push(send);

    // if there's a write op, our send will be picked up when the fd becomes writable
    // otherwise we can write immdiately
    if (!peer->write_op)
        epollout_peer(peer);
}

// try to read `len` bytes out of `fd` into `buf`
// this is a nonblocking operation and may only read some of the bytes
[[nodiscard]] IoResult readexact(int fd, uint8_t *buf, size_t len)
{
    size_t total = 0;

    while (total < len)
    {
        auto n = read(fd, buf + total, len - total);

        if (n < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return {
                    .tag = IoResult::Blocked,
                    .n_bytes = total,
                };

            if (errno == ECONNRESET || errno == EPIPE)
                return {
                    .tag = IoResult::Disconnect,
                    .n_bytes = total,
                };

            // todo: handle other errors?
            panic("read error: " + std::to_string(errno) + " ; fd: " + std::to_string(fd));
        }

        // graceful disconnect
        if (n == 0)
            return {
                .tag = IoResult::Disconnect,
                .n_bytes = total,
            };

        total += n;
    }

    return {
        .tag = IoResult::Done,
        .n_bytes = total,
    };
}

// read a message
// nonblocking, resumable
[[nodiscard]] ReadResult read_message(int fd, IoOperation *op)
{
    switch (op->progress)
    {
    case IoProgress::Magic:
    {
        auto result = readexact(fd, op->buffer + op->cursor, 4 - op->cursor);
        op->cursor += result.n_bytes;

        if (result.tag == IoResult::Disconnect)
            return ReadResult::Disconnect;

        if (result.tag == IoResult::Blocked)
            return ReadResult::Blocked;

        if (std::memcmp(op->buffer, MAGIC, 4) != 0)
            return ReadResult::BadMagic;

        op->progress = IoProgress::Type;
        op->cursor = 0;
    }
        [[fallthrough]];
    case IoProgress::Type:
    {
        uint8_t type;
        auto result = readexact(fd, &type, 1);

        if (result.tag == IoResult::Disconnect)
            return ReadResult::Disconnect;

        if (result.tag == IoResult::Blocked)
            return ReadResult::Blocked;

        if (type > (uint8_t)MessageType::Disconnect)
            return ReadResult::BadType;

        op->type = (MessageType)type;

        op->progress = IoProgress::Header;
        op->cursor = 0;
    }
        [[fallthrough]];
    case IoProgress::Header:
    {
        auto result = readexact(fd, op->buffer + op->cursor, 4 - op->cursor);
        op->cursor += result.n_bytes;

        if (result.tag == IoResult::Disconnect)
            return ReadResult::Disconnect;

        if (result.tag == IoResult::Blocked)
            return ReadResult::Blocked;

        uint32_t len;
        deserialize_u32(op->buffer, &len);
        len = ntohl(len);

        op->progress = IoProgress::Payload;
        op->cursor = 0;
        op->payload = Bytes::alloc(len);
    }
        [[fallthrough]];
    case IoProgress::Payload:
    {
        auto result = readexact(fd, op->payload.data + op->cursor, op->payload.len - op->cursor);
        op->cursor += result.n_bytes;

        if (result.tag == IoResult::Disconnect)
            return ReadResult::Disconnect;

        if (result.tag == IoResult::Blocked)
            return ReadResult::Blocked;

        return ReadResult::Read;
    }
    }

    panic("unreachable");
}

void remove_peer(RawPeer *peer)
{
    peer->connector->thread->remove_peer(peer);
    peer->connector->remove_peer(peer);

    if (peer->write_op)
    {
        // note: write_op's payload is not freed, because it's owned by the Python thread sending the message
        // peer->write_op->payload.free();
        peer->write_op = std::nullopt;
    }

    if (peer->read_op)
    {
        peer->read_op->payload.free();
        peer->read_op = std::nullopt;
    }

    close(peer->fd);
    peer->fd = -1;

    peer->state = PeerState::Disconnected;
}

// must return to epoll_wait() after calling this
// this frees the peer's EpollData and deletes the *peer
void reconnect_peer(RawPeer *peer)
{
    remove_peer(peer);

    // retry the connection if we're the connector
    if (peer->type == PeerType::Connector)
    {
        auto thread = peer->connector->thread;

        peer->identity.free();
        peer->identity = Bytes::empty();

        thread->connecting.push_back(peer);
        thread->ensure_timer_armed();
    }
    else
    {
        delete peer;
    }
}

// --- public api ---

void network_connector_init(Session *session, NetworkConnector *connector, Transport transport, ConnectorType type, uint8_t *identity, size_t len)
{
    new (connector) NetworkConnector{
        .type = type,
        .transport = transport,
        .thread = session->next_thread(),
        .session = session,
        .identity = Bytes::copy(identity, len),
        .rr = 0,
        .fd = -1,
        .addr = std::nullopt,
        .peers = std::vector<RawPeer *>(),
        .send_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .send_queue = ConcurrentQueue<SendMessage>(),
        .recv_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .recv_queue = ConcurrentQueue<void *>(),
        .recv_buffer_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .recv_buffer = ConcurrentQueue<Message>(),
        .destroy_tfd = -1,
        .destroy = std::nullopt,
    };

    connector->thread->add_connector(connector);
}

void network_connector_bind_tcp(NetworkConnector *connector, const char *host, uint16_t port)
{
    sockaddr_storage address;
    std::memset(&address, 0, sizeof(address));

    connector->fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);

    if (connector->fd < 0)
        panic("failed to create tcp socket: " + std::to_string(errno));

    set_sock_opts(connector->fd);

    in_addr_t in_addr = strcmp(host, "*") ? inet_addr(host) : INADDR_ANY;

    if (in_addr == INADDR_NONE)
        panic("failed to parse address: " + std::string(host));

    *(sockaddr_in *)&address = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {
            .s_addr = in_addr},
        .sin_zero = {0},
    };

    connector->addr = address;

    if (bind(connector->fd, (sockaddr *)&address, sizeof(sockaddr_in)) < 0)
    {
        if (errno == EADDRINUSE)
            panic("address in use: " + std::string(host) + ":" + std::to_string(port));

        panic("failed to bind socket: " + std::to_string(errno));
    }
}

void network_connector_bind_unix(NetworkConnector *connector, const char *path)
{
    sockaddr_storage address;
    std::memset(&address, 0, sizeof(address));

    connector->fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);

    if (connector->fd < 0)
        panic("failed to create unix socket: " + std::to_string(errno));

    // remove the previous lock from the fs
    if (unlink(path) < 0 && errno != ENOENT)
        panic("failed to unlink unix socket: " + std::to_string(errno));

    auto addr_un = (sockaddr_un *)&address;

    *addr_un = {
        .sun_family = AF_UNIX,
        .sun_path = {0}};

    std::strncpy(addr_un->sun_path, path, sizeof(addr_un->sun_path) - 1);

    if (bind(connector->fd, (sockaddr *)addr_un, sizeof(sockaddr_un)) < 0)
        panic("failed to bind socket: " + std::to_string(errno));
}

void network_connector_bind(NetworkConnector *connector, const char *host, uint16_t port)
{
    if (connector->fd > 0)
        panic("connector already bound");

    sockaddr_storage address;
    std::memset(&address, 0, sizeof(address));

    switch (connector->transport)
    {
    case Transport::TCP:
        network_connector_bind_tcp(connector, host, port);
        break;
    case Transport::InterProcess:
        network_connector_bind_unix(connector, host);
        break;
    case Transport::IntraProcess:
        panic("Client does not support IntraProcess transport");
    }

    if (listen(connector->fd, 16) < 0)
        panic("failed to listen on socket");

    connector->thread->add_epoll(connector->fd, EPOLLIN | EPOLLET, EpollType::ConnectorListener, connector);
}

void network_connector_connect(NetworkConnector *connector, const char *addr, uint16_t port)
{
    sockaddr_storage address;
    std::memset(&address, 0, sizeof(address));

    switch (connector->transport)
    {
    case Transport::InterProcess:
    {
        auto addr_un = (sockaddr_un *)&address;

        *addr_un = {
            .sun_family = AF_UNIX,
            .sun_path = {0}};

        std::strncpy(addr_un->sun_path, addr, sizeof(addr_un->sun_path) - 1);
    }
    break;
    case Transport::TCP:
    {
        *(sockaddr_in *)&address = {
            .sin_family = AF_INET,
            .sin_port = htons(port),
            .sin_addr = {
                .s_addr = inet_addr(addr)},
            .sin_zero = {0},
        };
    }
    break;
    case Transport::IntraProcess:
        panic("Client does not support IntraProcess transport");
    }

    auto peer = new RawPeer{
        .connector = connector,
        .identity = Bytes::empty(),
        .addr = address,
        .type = PeerType::Connector,
        .fd = -1, // a real fd will be assigned later
        .queue = std::queue<SendMessage>(),
        .state = PeerState::Disconnected,
        .read_op = std::nullopt,
        .write_op = std::nullopt,
    };

    ControlRequest request{
        .op = ControlOperation::Connect,
        .completer = Completer::none(),
        .peer = peer,
    };

    connector->thread->control(request);
}

void network_connector_send(void *future, NetworkConnector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len)
{
    if (connector->type == ConnectorType::Sub)
        panic("clients of type 'sub' do not support sending messages");

    // this data is owned by the caller,
    // but it's kept alive until the future is resolved
    SendMessage send{
        .completer = Completer::future(future),
        .msg = {
            .type = MessageType::Data,
            .address = {
                .data = to,
                .len = to_len,
            },
            .payload = {
                .data = data,
                .len = data_len,
            },
        },
    };

    connector->send_queue.enqueue(send);

    if (eventfd_signal(connector->send_event_fd) < 0)
        panic("failed to write to eventfd: " + std::to_string(errno));
}

void network_connector_send_sync(NetworkConnector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len)
{
    if (connector->type == ConnectorType::Sub)
        panic("clients of type 'sub' do not support sending messages");

    sem_t *sem = (sem_t *)std::malloc(sizeof(sem_t));

    if (sem_init(sem, 0, 0) < 0)
        panic("failed to initialize semaphore: " + std::to_string(errno));

    SendMessage send{
        .completer = Completer::semaphore(sem),
        .msg = {
            .type = MessageType::Data,
            .address = Bytes::copy(to, to_len),
            .payload = Bytes::copy(data, data_len),
        },
    };

    connector->send_queue.enqueue(send);

    if (eventfd_signal(connector->send_event_fd) < 0)
        panic("failed to write to eventfd: " + std::to_string(errno));

    if (sem_wait(sem) < 0)
    {
        if (sem_destroy(sem) < 0)
            panic("failed to destroy semaphore: " + std::to_string(errno));
        std::free(sem);

        if (errno == EINTR)
            return;

        panic("failed to await semaphore: " + std::to_string(errno));
    }

    if (sem_destroy(sem) < 0)
        panic("failed to destroy semaphore: " + std::to_string(errno));
    std::free(sem);
}

void network_connector_recv(void *future, NetworkConnector *connector)
{
    connector->recv_queue.enqueue(future);

    if (eventfd_signal(connector->recv_event_fd) < 0)
        panic("failed to write to eventfd: " + std::to_string(errno));
}

void network_connector_recv_sync(NetworkConnector *connector, Message *msg)
{
wait:
    if (auto code = fd_wait(connector->recv_buffer_event_fd, -1, POLLIN))
        panic("fd_wait(): " + std::to_string(code) + " ; " + std::to_string(errno));

    if (eventfd_wait(connector->recv_buffer_event_fd) < 0)
    {
        if (errno == EAGAIN)
            goto wait; // pre-empted

        panic("failed to read eventfd: " + std::to_string(errno));
    }

    while (!connector->recv_buffer.try_dequeue(*msg))
        ; // wait
}

void network_connector_destroy([[maybe_unused]] NetworkConnector *connector)
{
    sem_t *sem = (sem_t *)std::malloc(sizeof(sem_t));

    if (sem_init(sem, 0, 0) < 0)
        panic("failed to initialize semaphore: " + std::to_string(errno));

    ControlRequest request{
        .op = ControlOperation::DestroyConnector,
        .completer = Completer::semaphore(sem),
        .connector = connector,
    };

    connector->thread->control(request);

wait:
    if (sem_wait(sem) < 0)
    {
        if (errno == EINTR)
            goto wait; // just wait again

        panic("failed to await semaphore: " + std::to_string(errno));
    }

    if (sem_destroy(sem) < 0)
        panic("failed to destroy semaphore: " + std::to_string(errno));
    std::free(sem);

    // call the destructor in-place
    connector->~NetworkConnector();
}
