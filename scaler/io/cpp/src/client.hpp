#ifndef CLIENT_H
#define CLIENT_H

// C
#include <cmath>

// C++
#include <optional>
#include <vector>
#include <expected>
#include <semaphore>

// System
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>

// Third-party
#include "third_party/concurrentqueue.h"

// Common
#include "common.hpp"

using moodycamel::ConcurrentQueue;

// --- declarations ---

struct Client;
struct Peer;
struct IoResult;
ENUM ReadResult : uint8_t;
ENUM WriteResult : uint8_t;
ENUM PeerType : uint8_t;
ENUM ConnectorType : uint8_t;
ENUM Transport : uint8_t;
ENUM PeerState : uint8_t;

// First-party
#include "session.hpp"

[[nodiscard]] IoResult writeall(int fd, uint8_t *data, size_t len);
[[nodiscard]] WriteResult write_message(int fd, IoOperation *op);
[[nodiscard]] IoResult readexact(int fd, uint8_t *buf, size_t len);
[[nodiscard]] ReadResult read_message(int fd, IoOperation *op);

void write_to_peer(Peer *peer, Bytes payload, Completer completer);
void reconnect_peer(Peer *peer);

void client_init(struct Session *session, struct Client *client, enum Transport transport, uint8_t *identity, size_t len, enum ConnectorType type);
void client_bind(struct Client *client, const char *host, uint16_t port);
void client_connect(struct Client *client, const char *addr, uint16_t port);
void client_send(void *future, struct Client *client, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void client_send_sync(struct Client *client, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void client_recv(void *future, struct Client *client);
void client_recv_sync(struct Client *client, struct Message *msg);
void client_destroy(struct Client *client);

// --- structs ---

struct SendMessage
{
    // resolved when the message is send
    Completer completer;

    // the message to send
    Message msg;
};

ENUM ConnectorType : uint8_t{
                         Pair,
                         Pub,
                         Sub,
                         Dealer,
                         Router};

// Clients are tcp or unix domain sockets (uds, ipc)
// no variant for in-process because they're handled separately
ENUM Transport : uint8_t{
                     TCP,
                     IntraProcess,
                     InterProcess};

struct Client
{
    ConnectorType type;
    Transport transport;

    ThreadContext *thread; // the thread that this client is bound to
    Session *session;      // backreference to session
    Bytes identity;        // the identity of this client

    size_t rr; // round robin for dealer

    int fd;                               // the bound socket, <0 when not bound
    std::optional<sockaddr_storage> addr; // addr for when we're bound
    std::vector<Peer *> peers;

    int unmuted_event_fd; // event fd for when the client is no longer muted

    int send_event_fd;                       // event fd for send queue
    ConcurrentQueue<SendMessage> send_queue; // the send queue for Python thread -> io thread communication
    int recv_event_fd;                       // event fd for recv queue
    ConcurrentQueue<void *> recv_queue;      // the recv queue for io thread -> Python thread communication
    int recv_buffer_event_fd;                // event fd for recv buffer, only needed for sync clients
    ConcurrentQueue<Message> recv_buffer;    // these are messages that have been received

    // must hold mutex
    bool peer_by_id(Bytes id, Peer **peer);
    void remove_peer(Peer *peer);
    bool muted();
    size_t peer_rr();
    void recv_msg(Message &&msg);
    void unmute();

    // send a message to a peer according to the client type's rules
    // - must have exclusive access to the client
    // - client must not be muted
    // - if the peer disconnects, a reconnect is attempted, but the message will be lost
    void send(SendMessage send);
};

ENUM PeerType : uint8_t{
                    // we connected to the remote
                    Connector,

                    // the remote connected to us
                    Connectee};

ENUM PeerState : uint8_t{
                     Connecting,
                     Connected,
                     Disconnected,
                 };

struct Peer
{
    Client *client;        // the binder that this peer belongs to
    Bytes identity;        // the peer's address, i.e. identity
    sockaddr_storage addr; // the peer's address
    PeerType type;         // the type of peer
    int fd;                // the socket fd of this peer

    PeerState state; // the state of the peer

    std::optional<IoOperation> read_op;  // the current read operation
    std::optional<IoOperation> write_op; // the current write operation

    void recv_msg(Bytes payload);
};

struct IoResult
{
    ENUM Tag{
        Done,       // the read or write is complete
        Blocked,    // the operation blocked, but some progress may have been made
        Disconnect, // the connection was lost
    } tag;

    size_t n_bytes;
};

ENUM WriteResult : uint8_t{
                       Done1,       // the read or write is complete
                       Blocked1,    // the operation blocked, but some progress may have been made
                       Disconnect1, // the connection was lost
                   };

ENUM ReadResult : uint8_t{
                      Read,        // data was read
                      Blocked2,    // we might have read some data, but the fd blocked
                      Disconnect2, // the connection was lost
                      BadMagic,    // the magic didn't match
                  };

#endif
#if INCLUDE_DEFS

// --- functions ---

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
            panic("pair: muted");

        auto peer = this->peers[0];

        write_to_peer(peer, send.msg.payload, send.completer);
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

        write_to_peer(peer, send.msg.payload, send.completer);
    }
    break;
    case ConnectorType::Pub:
    {
        std::cout << "pub: " << this->identity.as_string() << ": sending message to " << std::to_string(this->peers.size()) << " peers" << std::endl;

        // if the socket has no peers, the message is dropped
        // we need to copy the peers because the vector may be modified
        for (auto peer : std::vector(this->peers))
            write_to_peer(peer, send.msg.payload, send.completer);
    }
    break;
    case ConnectorType::Dealer:
    {
        std::cout << "dealer: " << this->identity.as_string() << ": sending message" << std::endl;

        if (this->peers.empty())
            panic("dealer: muted");

        // dealers round-robin their peers
        auto peer = this->peers[this->peer_rr()];

        write_to_peer(peer, send.msg.payload, send.completer);
    }
    break;
    default:
        panic("unknown client type");
    }
}

void Peer::recv_msg(Bytes payload)
{
    Message message{
        .address = this->identity,
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

// note: peer may be in reconnecting state after calling this
// the peer's EpollData may have been freed
void write_to_peer(Peer *peer, Bytes payload, Completer completer)
{
    auto op = IoOperation::write(payload, completer);
    auto result = write_message(peer->fd, &op);

    switch (result)
    {
    case WriteResult::Disconnect1:
    {
        std::cout << "write_to_peer(): disconnect" << std::endl;

        reconnect_peer(peer);
        return;
    }
    case WriteResult::Blocked1:
    {
        std::cout << "write_to_peer(): blocked" << std::endl;

        // save the write operation
        peer->write_op = op;
        return;
    }
    case WriteResult::Done1:
    {
        std::cout << "write_to_peer(): done" << std::endl;

        // the write is complete
        break;
    }
    }
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
            panic("read error: " + std::to_string(errno));
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
        op->payload = Bytes{
            .data = (uint8_t *)malloc(len),
            .len = len,
        };
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
    }
        return ReadResult::Read;
    }

    panic("unreachable");
}

// must return to epoll_wait() after ccalling this
// this frees the peer's EpollData and deletes the *peer
void reconnect_peer(Peer *peer)
{
    auto client = peer->client;
    auto thread = client->thread;

    thread->remove_peer(peer);
    client->remove_peer(peer);

    std::cout << "closing fd: " << std::to_string(peer->fd) << std::endl;
    close(peer->fd);

    // retry the connection if we're the connector
    if (peer->type == PeerType::Connector)
    {
        auto thread = peer->client->thread;

        free(peer->identity.data);
        peer->identity = {
            .data = nullptr,
            .len = 0,
        };
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

void client_init(struct Session *session, struct Client *client, enum Transport transport, uint8_t *identity, size_t len, enum ConnectorType type)
{
    uint8_t *identity_dup = (uint8_t *)malloc(len * sizeof(uint8_t));
    std::memcpy(identity_dup, identity, len);

    new (client) Client{
        .type = type,
        .transport = transport,
        .thread = session->next_thread(),
        .session = session,
        .identity = Bytes{
            .data = identity_dup,
            .len = len,
        },
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
    };

    client->thread->add_client(client);
}

void client_bind(struct Client *client, const char *host, uint16_t port)
{
    int fd = -1, status = -1;
    sockaddr_storage addr;

    switch (client->transport)
    {
    case Transport::InterProcess:
    {
        fd = socket(
            AF_UNIX,
            SOCK_STREAM | SOCK_NONBLOCK,
            0);

        if (fd < 0)
            panic("failed to create socket: " + std::to_string(errno));

        sockaddr_un server_addr{
            .sun_family = AF_UNIX,
            .sun_path = {0}};

        std::strncpy(server_addr.sun_path, host, sizeof(server_addr.sun_path) - 1);
        std::memcpy(&addr, &server_addr, sizeof(server_addr));

        status = bind(fd, (sockaddr *)&server_addr, sizeof(server_addr));
    }
    break;
    case Transport::TCP:
    {
        fd = socket(
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK,
            0);

        if (fd < 0)
            panic("failed to create socket: " + std::to_string(errno));

        set_sock_opts(fd);

        in_addr_t in_addr = strncmp(host, "*", 1) ? inet_addr(host) : INADDR_ANY;

        sockaddr_in server_addr{
            .sin_family = AF_INET,
            .sin_port = htons(port),
            .sin_addr = {
                .s_addr = in_addr},
            .sin_zero = {0},
        };

        std::memcpy(&addr, &server_addr, sizeof(server_addr));
        status = bind(fd, (sockaddr *)&server_addr, sizeof(server_addr));
    }
    break;
    case Transport::IntraProcess:
        panic("Client does not support IntraProcess transport");
    }

    if (status < 0)
    {
        if (errno == EADDRINUSE)
        {
            panic("address in use: " + std::string(host) + ":" + std::to_string(port));
        }

        panic("failed to bind socket: " + std::to_string(errno));
    }

    if (listen(fd, 128) < 0)
    {
        panic("failed to listen on socket");
    }

    client->fd = fd;
    client->addr = addr;

    client->thread->add_epoll(client->fd, EPOLLIN | EPOLLET, EpollType::ClientListener, client);

    std::cout << "client: " << client->identity.as_string() << ": bound to: " << host << ":" << std::to_string(port) << std::endl;
}

void client_connect(struct Client *client, const char *addr, uint16_t port)
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
        .sem = std::nullopt,
        .addr = address,
        .client = client,
    };

    client->thread->control(request);
}

void client_send(void *future, struct Client *client, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len)
{
    SendMessage send{
        .completer = Completer::future(future),
        .msg = {
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

    client->send_queue.enqueue(send);

    if (eventfd_signal(client->send_event_fd) < 0)
        panic("failed to write to eventfd: " + std::to_string(errno));
}

void client_send_sync(struct Client *client, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len)
{
    // wait for the client to be unmuted
    if (client->muted())
    {
        if (fd_wait(client->unmuted_event_fd, -1, POLLIN) < 0)
            panic("failed to wait for unmuted event: " + std::to_string(errno));

        if (eventfd_wait(client->unmuted_event_fd) < 0)
            panic("failed to read eventfd: " + std::to_string(errno));
    }

    auto sem = std::binary_semaphore(0);

    SendMessage send{
        .completer = Completer::semaphore(&sem),
        .msg = {
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

    client->send(send);
    sem.acquire();
}

void client_recv(void *future, struct Client *client)
{
    client->recv_queue.enqueue(future);

    if (eventfd_signal(client->recv_event_fd) < 0)
        panic("failed to write to eventfd: " + std::to_string(errno));
}

void client_recv_sync(struct Client *client, struct Message *msg)
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
    // panic("todo: implement client_destroy: " + client->identity.as_string());
}

#endif
