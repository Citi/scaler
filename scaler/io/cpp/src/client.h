#ifndef CLIENT_H
#define CLIENT_H

// C
#include <cmath>

// C++
#include <optional>
#include <vector>
#include <expected>

// System
#include <sys/socket.h>
#include <arpa/inet.h>

// Third-party
#include "third_party/concurrentqueue.h"

// Common
#include "common.h"

using moodycamel::ConcurrentQueue;

// --- declarations ---

struct Client;
struct Peer;
struct SendMsg;
ENUM PeerType : uint8_t;
ENUM ConnectorType: uint8_t;
ENUM Transport: uint8_t;

// First-party
#include "session.h"

[[nodiscard]] std::optional<size_t> writeall(int fd, uint8_t *data, size_t len, bool nonblocking);
[[nodiscard]] std::optional<size_t> write_message(int fd, Bytes *payload, bool nonblocking);
[[nodiscard]] std::optional<size_t> readexact(int fd, uint8_t *buf, size_t len, bool nonblocking, int timeout);
[[nodiscard]] bool read_message(int fd, Bytes *payload, bool nonblocking);

void write_to_peer(Peer *peer, Bytes payload, void *future);
void reconnect_peer(Peer *peer);

// --- structs ---

struct SendMsg
{
    // the future to resolve when the message is sent
    void *future;

    // the message to send
    Message msg;
};

ENUM ConnectorType: uint8_t
{
    Pair,
    Pub,
    Sub,
    Dealer,
    Router
};

// Clients are tcp or unix domain sockets (uds, ipc)
// no variant for in-process because they're handled separately
ENUM Transport: uint8_t
{
    TCP,
    IntraProcess,
    InterProcess
};

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

    int send_event_fd;                    // event fd for send queue
    ConcurrentQueue<SendMsg> send_queue;  // the send queue for Python thread -> io thread communication
    int recv_event_fd;                    // event fd for recv queue
    ConcurrentQueue<void *> recv_queue;   // the recv queue for io thread -> Python thread communication
    int recv_buffer_event_fd;             // event fd for recv buffer, only needed for sync clients
    ConcurrentQueue<Message> recv_buffer; // these are messages that have been received

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
    void send(SendMsg send);
};

ENUM PeerType: uint8_t
{
    // we connected to the remote
    Connector,

    // the remote connected to us
    Connectee
};

struct Peer
{
    Client *client;        // the binder that this peer belongs to
    Bytes identity;        // the peer's address, i.e. identity
    sockaddr_storage addr; // the peer's address
    PeerType type;         // the type of peer
    int fd;                // the socket fd of this peer

    void save_write(WriteOperation op);
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

void Client::recv_msg(Message &&msg) {
    panic("todo: " + msg.address.as_string());
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

        SendMsg send;
        while (!this->send_queue.try_dequeue(send))
            ; // wait

        this->send(send);
    };

    if (eventfd_signal(this->unmuted_event_fd) < 0)
    {
        panic("failed to write to eventfd: " + std::to_string(errno));
    }
}

void Client::send(SendMsg send)
{
    switch (this->type)
    {
    case ConnectorType::Pair:
    {
        if (this->peers.empty())
            panic("pair: muted");

        auto peer = this->peers[0];

        write_to_peer(peer, send.msg.payload, send.future);
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

        write_to_peer(peer, send.msg.payload, send.future);
    }
    break;
    case ConnectorType::Pub:
    {
        // if the socket has no peers, the message is dropped
        for (auto peer : this->peers)
            write_to_peer(peer, send.msg.payload, send.future);
    }
    break;
    case ConnectorType::Dealer:
    {
        if (this->peers.empty())
            panic("dealer: muted");

        // dealers round-robin their peers
        auto peer = this->peers[this->peer_rr()];

        write_to_peer(peer, send.msg.payload, send.future);
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

                continue;
            }

            // this is a disconnect
            if (errno == EPIPE || errno == ECONNRESET)
                return std::nullopt;

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
void write_to_peer(Peer *peer, Bytes payload, void *future)
{
    auto n_bytes = write_message(peer->fd, &payload, true);

    // disconnect
    if (!n_bytes)
        reconnect_peer(peer);

    // partial write, we need to resume later
    if (*n_bytes < payload.len)
    {
        auto op = WriteOperation{
            .future = future,
            .payload = payload,
            .cursor = *n_bytes,
        };

        peer->save_write(op);
    }
}

[[nodiscard]] std::optional<size_t> readexact(int fd, uint8_t *buf, size_t len, bool nonblocking, int timeout)
{
    size_t total = 0;

    while (total < len)
    {
        auto n = read(fd, buf + total, len - total);

        if (n < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                if (nonblocking)
                    return total;

                if (auto res = fd_wait(fd, timeout, POLLIN))
                {
                    if (res > 0)
                        panic("readexact(): received signal: " + std::to_string(res));

                    if (res == FdWait::Other)
                        panic("readexact(): poll failed: " + std::to_string(errno));

                    if (res == FdWait::Timeout)
                        return total;
                }

                continue;
            }

            // todo: handle other errors?
            panic("read error: " + std::to_string(errno));
        }

        if (n == 0)
            return std::nullopt;

        total += n;
    }

    return total;
}

// true -> success
// false -> failure
[[nodiscard]] bool read_message(int fd, Bytes *payload, bool nonblocking)
{
    uint8_t magic[4];
    auto n_bytes = readexact(fd, magic, 4, false, 2000);

    // n_bytes is empty if the connection was lost
    // of <4 if the read timed out
    if (!n_bytes || *n_bytes < 4)
        return false;

    // if the magic doesn't match we treat it as a disconnect
    if (std::memcmp(magic, MAGIC, 4) != 0)
        return false;

    uint8_t header[4];
    n_bytes = readexact(fd, header, 4, false, 2000);

    if (!n_bytes || *n_bytes < 4)
        return false;

    uint32_t len;
    deserialize_u32(header, &len);

    payload->len = ntohl(len);
    payload->data = (uint8_t *)malloc(payload->len);

    double timeout = 1500.0 * std::log(payload->len / 1024.0);
    n_bytes = readexact(fd, payload->data, payload->len, nonblocking, std::max(2000.0, timeout));

    if (!n_bytes || *n_bytes < payload->len)
        return false;

    return true;
}

void reconnect_peer(Peer *peer)
{
    auto client = peer->client;
    auto thread = client->thread;

    thread->remove_peer(peer);
    client->remove_peer(peer);

    close(peer->fd);

    // retry the connection if we're the connector
    if (peer->type == PeerType::Connector)
    {
        // todo: put a limit on the number of retries?
        client_connect_peer(thread, peer);
    }
}

// --- public api ---

#endif
