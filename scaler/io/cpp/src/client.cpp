#include "client.hpp"

bool Client::peer_by_id(Bytes id, Peer **peer)
{
    auto it = std::find_if(this->peers.begin(), this->peers.end(), [id](Peer *p)
                           { return p->identity == id; });

    if (it != this->peers.end())
    {
        *peer = *it;
        return true;
    }

    return false;
}

void Client::remove_peer(Peer *peer)
{
    std::erase(this->peers, peer);
}

bool Client::muted()
{
    // these types mute when they have no peers
    if (this->type == ConnectorType::Pair || this->type == ConnectorType::Dealer)
    {
        return this->peers.empty();
    }

    // other types drop messages when they have no peers
    return false;
}

size_t Client::peer_rr()
{
    auto rr = this->rr;
    this->rr = (this->rr + 1) % this->peers.size();

    return rr;
}

void Client::recv_msg(Message &&msg)
{
    // if there's a waiting recv, complete it immediately
    if (eventfd_wait(this->recv_event_fd) == 0)
    {
        std::cout << "Client::recv_msg(): completing future" << std::endl;

        void *future;
        while (!this->recv_queue.try_dequeue(future))
            ; // wait

        future_set_result(future, &msg);
        message_destroy(msg);
    }
    else
    {
        std::cout << "Client::recv_msg(): buffering message" << std::endl;

        // buffer the message
        this->recv_buffer.enqueue(msg);

        // support for sync clients
        if (eventfd_signal(this->recv_buffer_event_fd) < 0)
            panic("failed to write to eventfd: " + std::to_string(errno));
    }
};

void Client::unmute()
{
    for (;;)
    {
        if (this->muted())
            return;

        if (eventfd_wait(this->send_event_fd) < 0)
        {
            if (errno == EAGAIN)
                break;

            panic("failed to read eventfd: " + std::to_string(errno));
        }

        SendMessage send;
        while (!this->send_queue.try_dequeue(send))
            ; // wait

        this->send(send);
    };

    if (eventfd_signal(this->unmuted_event_fd) < 0)
        panic("failed to write to eventfd: " + std::to_string(errno));
}

// panics if the client is muted
void Client::send(SendMessage send)
{
    switch (this->type)
    {
    case ConnectorType::Pair:
    {
        std::cout << "pair: " << this->identity.as_string() << ": sending message" << std::endl;

        if (this->peers.empty())
            panic("client: muted");

        auto peer = this->peers[0];

        write_to_peer(peer, send);
    }
    break;
    case ConnectorType::Router:
    {
        std::cout << "router: " << this->identity.as_string() << ": sending message to: " << send.msg.address.as_string() << std::endl;

        Peer *peer;
        if (!this->peer_by_id(send.msg.address, &peer))
        {
            // routers drop messages
            break;
        }

        write_to_peer(peer, send);
    }
    break;
    case ConnectorType::Pub:
    {
        std::cout << "pub: " << this->identity.as_string() << ": sending message to " << std::to_string(this->peers.size()) << " peers" << std::endl;

        // if the socket has no peers, the message is dropped
        // we need to copy the peers because the vector may be modified
        for (auto peer : std::vector(this->peers))
            write_to_peer(peer, send);
    }
    break;
    case ConnectorType::Dealer:
    {
        std::cout << "dealer: " << this->identity.as_string() << ": sending message" << std::endl;

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

void Peer::recv_msg(Bytes payload)
{
    Message message{
        .type = MessageType::Data,
        .address = this->identity.ref(),
        .payload = payload,
    };

    this->client->recv_msg(std::move(message));
}

// attempt to write all data to an fd
// - returns the number of bytes written or empty if the connection was lost
// - if nonblocking is true, the function will return immediately if the fd blocks (EAGAIN or EWOULDBLOCK)
//   otherwise, the function will block until all data is written
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

[[nodiscard]] WriteResult write_message(int fd, IoOperation *op)
{
    switch (op->progress)
    {
    case IoProgress::Magic:
    {
        auto result = writeall(fd, MAGIC + op->cursor, 4 - op->cursor);
        op->cursor += result.n_bytes;

        if (result.tag == IoResult::Disconnect)
            return WriteResult::Disconnect1;

        if (result.tag == IoResult::Blocked)
            return WriteResult::Blocked1;

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
            return WriteResult::Disconnect1;

        if (result.tag == IoResult::Blocked)
            return WriteResult::Blocked1;

        op->progress = IoProgress::Payload;
        op->cursor = 0;
    }
        [[fallthrough]];
    case IoProgress::Type:
    {
        uint8_t type[] = {(uint8_t)op->type};

        auto result = writeall(fd, type, 1);

        if (result.tag == IoResult::Disconnect)
            return WriteResult::Disconnect1;

        if (result.tag == IoResult::Blocked)
            return WriteResult::Blocked1;

        op->progress = IoProgress::Payload;
        op->cursor = 0;
    }
        [[fallthrough]];
    case IoProgress::Payload:
    {
        auto result = writeall(fd, op->payload.data + op->cursor, op->payload.len - op->cursor);
        op->cursor += result.n_bytes;

        if (result.tag == IoResult::Disconnect)
            return WriteResult::Disconnect1;

        if (result.tag == IoResult::Blocked)
            return WriteResult::Blocked1;

        std::cout << "write_message(): wrote to: " << std::to_string(fd) << std::endl;

        return WriteResult::Done1;
    }
    }

    panic("unreachable");
}

// process the send queue until the socket blocks or the queue is exhausted
ControlFlow epollout_peer(Peer *peer)
{
    std::cout << "epollout()" << std::endl;

    for (;;)
    {
        if (!peer->write_op)
        {
            if (peer->queue.empty())
            {
                std::cout << "epollout(): queue exhausted" << std::endl;
                return ControlFlow::Continue; // queue exhausted
            }

            auto send = peer->queue.front();
            peer->queue.pop();

            peer->write_op = IoOperation::write(send.msg.payload, send.msg.type, send.completer);
        }

        auto result = write_message(peer->fd, &*peer->write_op);

        switch (result)
        {
        case WriteResult::Blocked1:
            std::cout << "epollout(): blocked" << std::endl;
            return ControlFlow::Continue;
        case WriteResult::Disconnect1:
            std::cout << "epollout(): disconnect" << std::endl;
            reconnect_peer(peer);
            return ControlFlow::Break; // we need to go back to epoll_wait() after calling reconnect_peer()
        case WriteResult::Done1:
            std::cout << "epollout(): wrote message" << std::endl;

            peer->write_op->complete();

            if (peer->write_op->type == MessageType::Disconnect && peer->client->destroy)
            {
                remove_peer(peer);
                if (peer->client->peers.empty())
                {
                    std::cout << "epollout(): CLIENT DESTROYED" << std::endl;
                    // the client is being destroyed and the last peer has disconnected

                    // TODO!!!!

                    peer->client->destroy->complete();
                }
                delete peer;

                return ControlFlow::Break;
            }

            peer->write_op = std::nullopt;
            return ControlFlow::Continue;
        }
    }
}

// note: peer may be in reconnecting state after calling this
// the peer's EpollData may have been freed
void write_to_peer(Peer *peer, SendMessage send)
{
    peer->queue.push(send);
    epollout_peer(peer);
    return;
}

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

[[nodiscard]] ReadResult read_message(int fd, IoOperation *op)
{
    switch (op->progress)
    {
    case IoProgress::Magic:
    {
        auto result = readexact(fd, op->buffer + op->cursor, 4 - op->cursor);
        op->cursor += result.n_bytes;

        if (result.tag == IoResult::Disconnect)
            return ReadResult::Disconnect2;

        if (result.tag == IoResult::Blocked)
            return ReadResult::Blocked2;

        if (std::memcmp((char *)op->buffer, MAGIC, 4) != 0)
            return ReadResult::BadMagic;

        op->progress = IoProgress::Header;
        op->cursor = 0;
    }
        [[fallthrough]];
    case IoProgress::Header:
    {
        auto result = readexact(fd, op->buffer + op->cursor, 4 - op->cursor);
        op->cursor += result.n_bytes;

        if (result.tag == IoResult::Disconnect)
            return ReadResult::Disconnect2;

        if (result.tag == IoResult::Blocked)
            return ReadResult::Blocked2;

        uint32_t len;
        deserialize_u32(op->buffer, &len);
        len = ntohl(len);

        op->progress = IoProgress::Payload;
        op->cursor = 0;
        op->payload = Bytes::alloc(len);
    }
        [[fallthrough]];
    case IoProgress::Type:
    {
        auto result = readexact(fd, (uint8_t *)&op->type, 1);

        if (result.tag == IoResult::Disconnect)
            return ReadResult::Disconnect2;

        if (result.tag == IoResult::Blocked)
            return ReadResult::Blocked2;

        op->progress = IoProgress::Payload;
        op->cursor = 0;
    }
        [[fallthrough]];
    case IoProgress::Payload:
    {
        auto result = readexact(fd, op->payload.data + op->cursor, op->payload.len - op->cursor);
        op->cursor += result.n_bytes;

        if (result.tag == IoResult::Disconnect)
            return ReadResult::Disconnect2;

        if (result.tag == IoResult::Blocked)
            return ReadResult::Blocked2;

        return ReadResult::Read;
    }
    }

    panic("unreachable");
}

void remove_peer(Peer *peer)
{
    auto client = peer->client;
    auto thread = client->thread;

    thread->remove_peer(peer);
    client->remove_peer(peer);

    std::cout << "closing fd: " << std::to_string(peer->fd) << std::endl;
    close(peer->fd);
}

// must return to epoll_wait() after ccalling this
// this frees the peer's EpollData and deletes the *peer
void reconnect_peer(Peer *peer)
{
    remove_peer(peer);

    // retry the connection if we're the connector
    if (peer->type == PeerType::Connector)
    {
        auto thread = peer->client->thread;

        peer->identity.free_();
        peer->identity = Bytes::empty();

        peer->fd = -1;
        peer->state = PeerState::Disconnected;
        thread->connecting.push_back(peer);

        if (!thread->timer_armed)
            thread->arm_timer();
    }
    else
    {
        delete peer;
    }
}

// --- public api ---

void client_init(Session *session, Client *client, Transport transport, uint8_t *identity, size_t len, ConnectorType type)
{
    new (client) Client{
        .type = type,
        .transport = transport,
        .thread = session->next_thread(),
        .session = session,
        .identity = Bytes::copy(identity, len),
        .rr = 0,
        .fd = -1,
        .addr = std::nullopt,
        .peers = std::vector<Peer *>(),
        .unmuted_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .send_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .send_queue = ConcurrentQueue<SendMessage>(),
        .recv_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .recv_queue = ConcurrentQueue<void *>(),
        .recv_buffer_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .recv_buffer = ConcurrentQueue<Message>(),
        .destroy = std::nullopt,
    };

    client->thread->add_client(client);
}

void client_bind(Client *client, const char *host, uint16_t port)
{
    if (client->fd > 0)
        panic("client already bound");

    sockaddr_storage address;

    switch (client->transport)
    {
    case Transport::InterProcess:
    {
        client->fd = socket(
            AF_UNIX,
            SOCK_STREAM | SOCK_NONBLOCK,
            0);

        if (client->fd < 0)
            panic("failed to create socket: " + std::to_string(errno));

        sockaddr_un server_addr{
            .sun_family = AF_UNIX,
            .sun_path = {0}};

        std::strncpy(server_addr.sun_path, host, sizeof(server_addr.sun_path) - 1);
        std::memcpy(&address, &server_addr, sizeof(server_addr));
    }
    break;
    case Transport::TCP:
    {
        client->fd = socket(
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK,
            0);

        if (client->fd < 0)
            panic("failed to create socket: " + std::to_string(errno));

        set_sock_opts(client->fd);

        in_addr_t in_addr = strcmp(host, "*") ? inet_addr(host) : INADDR_ANY;

        if (in_addr == INADDR_NONE)
            panic("failed to parse address: " + std::string(host));

        sockaddr_in server_addr{
            .sin_family = AF_INET,
            .sin_port = htons(port),
            .sin_addr = {
                .s_addr = in_addr},
            .sin_zero = {0},
        };

        std::memcpy(&address, &server_addr, sizeof(server_addr));
    }
    break;
    case Transport::IntraProcess:
        panic("Client does not support IntraProcess transport");
    }

    client->addr = address;

    if (bind(client->fd, (sockaddr *)&address, sizeof(address)) < 0)
    {
        if (errno == EADDRINUSE)
        {
            panic("address in use: " + std::string(host) + ":" + std::to_string(port));
        }

        panic("failed to bind socket: " + std::to_string(errno));
    }

    if (listen(client->fd, 128) < 0)
    {
        panic("failed to listen on socket");
    }

    client->thread->add_epoll(client->fd, EPOLLIN | EPOLLET, EpollType::ClientListener, client);

    std::cout << "client: " << client->identity.as_string() << ": bound to: " << host << ":" << std::to_string(port) << std::endl;
}

void client_connect(Client *client, const char *addr, uint16_t port)
{
    sockaddr_storage address;

    switch (client->transport)
    {
    case Transport::InterProcess:
    {
        sockaddr_un server_addr{
            .sun_family = AF_UNIX,
            .sun_path = {0}};

        std::strncpy(server_addr.sun_path, addr, sizeof(server_addr.sun_path) - 1);
        std::memcpy(&address, &server_addr, sizeof(server_addr));
    }
    break;
    case Transport::TCP:
    {
        std::cout << "connecting to: " << addr << ":" << std::to_string(port) << std::endl;

        sockaddr_in server_addr{
            .sin_family = AF_INET,
            .sin_port = htons(port),
            .sin_addr = {
                .s_addr = inet_addr(addr)},
            .sin_zero = {0},
        };

        std::memcpy(&address, &server_addr, sizeof(server_addr));
    }
    break;
    case Transport::IntraProcess:
        panic("Client does not support IntraProcess transport");
    }

    ControlRequest request{
        .op = ControlOperation::Connect,
        .completer = Completer::none(),
        .addr = address,
        .client = client,
    };

    client->thread->control(request);
}

void client_send(void *future, Client *client, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len)
{
    SendMessage send{
        .completer = Completer::future(future),
        .msg = {
            .type = MessageType::Data,
            .address = {
                .owned = false,
                .data = to,
                .len = to_len,
            },
            .payload = {
                .owned = false,
                .data = data,
                .len = data_len,
            },
        },
    };

    client->send_queue.enqueue(send);

    if (eventfd_signal(client->send_event_fd) < 0)
        panic("failed to write to eventfd: " + std::to_string(errno));
}

void client_send_sync(Client *client, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len)
{
    std::binary_semaphore sem(0);

    SendMessage send{
        .completer = Completer::semaphore(&sem),
        .msg = {
            .type = MessageType::Data,
            .address = {
                .owned = false,
                .data = to,
                .len = to_len,
            },
            .payload = {
                .owned = false,
                .data = data,
                .len = data_len,
            },
        },
    };

    client->send_queue.enqueue(std::move(send));

    if (eventfd_signal(client->send_event_fd) < 0)
        panic("failed to write to eventfd: " + std::to_string(errno));

    sem.acquire();
}

void client_recv(void *future, Client *client)
{
    client->recv_queue.enqueue(future);

    if (eventfd_signal(client->recv_event_fd) < 0)
        panic("failed to write to eventfd: " + std::to_string(errno));
}

void client_recv_sync(Client *client, Message *msg)
{
    if (fd_wait(client->recv_buffer_event_fd, -1, POLLIN) < 0)
        panic("failed to wait for recv buffer: " + std::to_string(errno));

    if (eventfd_wait(client->recv_buffer_event_fd) < 0)
        panic("failed to read eventfd: " + std::to_string(errno));

    while (!client->recv_buffer.try_dequeue(*msg))
        ; // wait
}

void client_destroy([[maybe_unused]] Client *client)
{
    auto sem = std::binary_semaphore(0);

    ControlRequest request{
        .op = ControlOperation::DestroyClient,
        .completer = Completer::semaphore(&sem),
        .addr = std::nullopt,
        .client = client,
    };

    client->thread->control(request);

    // wait for the client to be destroyed
    sem.acquire();

    // call the destructor in-place
    client->~Client();
}
