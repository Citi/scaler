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
#include "common.h"

using moodycamel::ConcurrentQueue;

// --- declarations ---

struct Client;
struct Peer;
struct SendMessage;
struct ReadMessage;
struct ReadResult;
struct ReadMessage;
ENUM ReadConfig : uint8_t;
ENUM PeerType : uint8_t;
ENUM ConnectorType : uint8_t;
ENUM Transport : uint8_t;

// First-party
#include "session.h"

[[nodiscard]] std::optional<size_t> writeall(int fd, uint8_t *data, size_t len, bool nonblocking);
[[nodiscard]] std::optional<size_t> write_message(int fd, Bytes *payload, bool nonblocking);
[[nodiscard]] ReadResult readexact(int fd, uint8_t *buf, size_t len, ReadConfig config, int timeout);
[[nodiscard]] ReadMessage read_message(int fd, bool nonblocking);

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

struct Peer
{
    Client *client;        // the binder that this peer belongs to
    Bytes identity;        // the peer's address, i.e. identity
    sockaddr_storage addr; // the peer's address
    PeerType type;         // the type of peer
    int fd;                // the socket fd of this peer

    void save_write(WriteOperation op);
    void save_read(ReadOperation op);

    void recv_msg(Bytes payload);
};

ENUM ReadConfig : uint8_t{
                      Nonblock,  // read will return immediately if the fd blocks
                      HardBlock, // read will block until all data is read, or timeout
                      SoftBlock, // read will block until all data is read, or exit with NoData if the fd blocks before any data is read
                  };

struct ReadResult
{
    // the type of result
    ENUM Tag{
        Read,       // successfully read all bytes (all)
        Blocked,    // nonblocking read blocked (nonblocking)
        Timeout,    // the read timed out (softblock or hardblock)
        Disconnect, // the connection was lost (all)
        NoData,     // there was no data available (EAGAIN || EWOULDBLOCK) (softblock)
    } tag;

    // always valid
    size_t n_bytes;
};

struct ReadMessage
{
    ENUM Tag{
        Read,       // the message was read, possibly partially
        Timeout,    // the read timed out
        Disconnect, // the connection was lost
        NoData,     // there was no data available (EAGAIN || EWOULDBLOCK)
        BadMagic,   // the magic didn't match
    } tag;

    // only valid when tag == Read
    ReadOperation op = {};
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
        void *future;
        while (!this->recv_queue.try_dequeue(future))
            ; // wait

        future_set_result(future, &msg);
        message_destroy(msg);
    }
    else
    {
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
        if (this->peers.empty())
            panic("pair: muted");

        auto peer = this->peers[0];

        write_to_peer(peer, send.msg.payload, send.completer);
    }
    break;
    case ConnectorType::Router:
    {
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
        // if the socket has no peers, the message is dropped
        // we need to copy the peers because the vector may be modified
        for (auto peer : std::vector(this->peers))
            write_to_peer(peer, send.msg.payload, send.completer);
    }
    break;
    case ConnectorType::Dealer:
    {
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

void Peer::save_write(WriteOperation op)
{
    this->client->thread->save_write(this, op);
}

void Peer::save_read(ReadOperation op)
{
    this->client->thread->save_read(this, op);
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
[[nodiscard]] std::optional<size_t> writeall(int fd, uint8_t *data, size_t len, bool nonblocking)
{
    size_t total = 0;

    while (total < len)
    {
        auto n = write(fd, data + total, len - total);

        if (n < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                if (nonblocking)
                    return total;

                if (auto res = fd_wait(fd, 2000, POLLOUT))
                {
                    if (res > 0)
                        panic("readexact(): received signal: " + std::to_string(res));

                    if (res == FdWait::Other)
                        panic("readexact(): poll failed: " + std::to_string(errno));

                    if (res == FdWait::Timeout)
                    {
                        std::cout << "writeall(): timeout" << std::endl;
                        return std::nullopt;
                    }
                }
            }

            // this is a disconnect
            if (errno == EPIPE || errno == ECONNRESET)
            {
                std::cout << "writeall(): EPIPE or ECONNRESET" << std::endl;
                return std::nullopt;
            }

            // todo: handle other errors?
            panic("write error: " + std::to_string(errno));
        }

        total += n;
    }

    return total;
}

// returns the number of payload bytes written or empty if the connection was lost
// blocks until message and header are written
// if nonblocking is true, the function will return immediately if the fd blocks (EAGAIN or EWOULDBLOCK)
// otherwise, the function will block until all data is written
[[nodiscard]] std::optional<size_t> write_message(int fd, Bytes *payload, bool nonblocking)
{
    if (!writeall(fd, MAGIC, 4, false))
        return std::nullopt;

    uint8_t header[4];
    serialize_u32(htonl((uint32_t)payload->len), header);

    if (!writeall(fd, header, 4, false))
        return std::nullopt;

    return writeall(fd, payload->data, payload->len, nonblocking);
}

// perform a non-blocking write to a peer
void write_to_peer(Peer *peer, Bytes payload, Completer completer)
{
    auto n_bytes = write_message(peer->fd, &payload, true);

    // disconnect
    if (!n_bytes)
    {
        std::cout << "write_to_peer(): disconnect" << std::endl;

        reconnect_peer(peer);
        return;
    }

    // partial write, we need to resume later
    if (*n_bytes < payload.len)
    {
        auto op = WriteOperation{
            .completer = completer,
            .payload = payload,
            .cursor = *n_bytes,
        };

        peer->save_write(op);
    }
}

[[nodiscard]] ReadResult readexact(int fd, uint8_t *buf, size_t len, ReadConfig config, int timeout)
{
    size_t total = 0;

    while (total < len)
    {
        auto n = read(fd, buf + total, len - total);

        if (n < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                if (config == ReadConfig::Nonblock)
                    return {
                        .tag = ReadResult::Blocked,
                        .n_bytes = total,
                    };

                if (config == ReadConfig::SoftBlock && total == 0)
                    return {
                        .tag = ReadResult::NoData,
                        .n_bytes = total,
                    };

                if (auto res = fd_wait(fd, timeout, POLLIN))
                {
                    if (res > 0)
                        panic("readexact(): received signal: " + std::to_string(res));

                    if (res == FdWait::Other)
                        panic("readexact(): poll failed: " + std::to_string(errno));

                    if (res == FdWait::Timeout)
                        return {
                            .tag = ReadResult::Timeout,
                            .n_bytes = total,
                        };
                }

                continue;
            }

            if (errno == ECONNRESET || errno == EPIPE)
                return {
                    .tag = ReadResult::Disconnect,
                    .n_bytes = total,
                };

            // todo: handle other errors?
            panic("read error: " + std::to_string(errno));
        }

        // graceful disconnect
        if (n == 0)
            return {
                .tag = ReadResult::Disconnect,
                .n_bytes = total,
            };

        total += n;
    }

    return {
        .tag = ReadResult::Read,
        .n_bytes = total,
    };
}

[[nodiscard]] ReadMessage read_message(int fd, bool nonblocking)
{
    {
        uint8_t magic[4];
        auto result = readexact(fd, magic, 4, nonblocking ? ReadConfig::SoftBlock : ReadConfig::HardBlock, 2000);

        if (result.tag == ReadResult::NoData)
            return {.tag = ReadMessage::NoData};

        if (result.tag == ReadResult::Disconnect)
            return {.tag = ReadMessage::Disconnect};

        if (result.tag == ReadResult::Timeout)
            return {.tag = ReadMessage::Timeout};

        // the tag must be ReadResult::Read, and we're soft blocking so we read the whole magic

        if (std::memcmp(magic, MAGIC, 4) != 0)
            return {.tag = ReadMessage::BadMagic};
    }

    uint32_t len;

    {
        uint8_t header[4];
        auto result = readexact(fd, header, 4, ReadConfig::HardBlock, 2000);

        if (result.tag == ReadResult::Disconnect)
            return {.tag = ReadMessage::Disconnect};

        if (result.tag == ReadResult::Timeout)
            return {.tag = ReadMessage::Timeout};

        deserialize_u32(header, &len);
        len = ntohl(len);
    }

    ReadOperation op{
        .payload = Bytes{
            .data = (uint8_t *)malloc(len),
            .len = len,
        },
        .cursor = 0,
    };

    {
        double timeout = 1500.0 * std::log(len / 1024.0);
        auto result = readexact(fd, op.payload.data, op.payload.len, nonblocking ? ReadConfig::Nonblock : ReadConfig::HardBlock, std::max(2000.0, timeout));

        if (result.tag == ReadResult::Disconnect)
            return {.tag = ReadMessage::Disconnect};

        // blocking
        if (result.tag == ReadResult::Timeout)
            return {.tag = ReadMessage::Timeout};

        op.cursor = result.n_bytes;
    }

    return {
        .tag = ReadMessage::Read,
        .op = op,
    };
}

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
        // todo: put a limit on the number of retries?
        client_connect_peer(thread, peer);
    } else {
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
        in_addr_t in_addr = strncmp(addr, "*", 1) ? inet_addr(addr) : INADDR_ANY;

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
        .completer = {
            .type = Completer::Future,
            .future = future,
        },
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

    // todo, support semaphore completion?
    // - this or need some other way to synchronously wait for the message to be sent
    SendMessage send{
        .completer = {
            .type = Completer::Semaphore,
            .sem = &sem},
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

void client_destroy(struct Client *client)
{
    panic("todo: implement client_destroy: " + client->identity.as_string());
}

#endif
