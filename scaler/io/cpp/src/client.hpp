#pragma once

// C
#include <cmath>

// C++
#include <optional>
#include <vector>

// System
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <semaphore.h>

// Third-party
#include "third_party/concurrentqueue.h"

// First-party
#include "common.hpp"
#include "session.hpp"

using moodycamel::ConcurrentQueue;

// --- declarations ---

struct Client;
struct Peer;
struct IoResult;
struct SendMessage;
enum class ReadResult;
enum class WriteResult;
enum class PeerType;
enum class PeerState;

ENUM ConnectorType : uint8_t;
ENUM Transport : uint8_t;

[[nodiscard]] IoResult writeall(int fd, uint8_t *data, size_t len);
[[nodiscard]] WriteResult write_message(int fd, IoOperation *op);
[[nodiscard]] IoResult readexact(int fd, uint8_t *buf, size_t len);
[[nodiscard]] ReadResult read_message(int fd, IoOperation *op);

void write_to_peer(Peer *peer, SendMessage send);
void reconnect_peer(Peer *peer);
void remove_peer(Peer *peer);
ControlFlow epollin_peer(Peer *peer);
ControlFlow epollout_peer(Peer *peer);

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

    int send_event_fd;                       // event fd for send queue
    ConcurrentQueue<SendMessage> send_queue; // the send queue for Python thread -> io thread communication
    int recv_event_fd;                       // event fd for recv queue
    ConcurrentQueue<void *> recv_queue;      // the recv queue for io thread -> Python thread communication
    int recv_buffer_event_fd;                // event fd for recv buffer, only needed for sync clients
    ConcurrentQueue<Message> recv_buffer;    // these are messages that have been received

    int destroy_tfd;
    std::optional<Completer> destroy; // this is used to wait for the destruction of a client

    // must hold mutex
    bool peer_by_id(Bytes id, Peer **peer);
    void remove_peer(Peer *peer);
    bool muted();
    size_t peer_rr();
    void recv_msg(Message &msg);
    void unmute();

    // send a message to a peer according to the client type's rules
    // - must have exclusive access to the client
    void send(SendMessage send);
};

enum class PeerType
{
    // we connected to the remote
    Connector,

    // the remote connected to us
    Connectee
};

enum class PeerState
{
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

    std::queue<SendMessage> queue; // messages to be sent by this peer

    PeerState state; // the state of the peer

    std::optional<IoOperation> read_op;  // the current read operation
    std::optional<IoOperation> write_op; // the current write operation

    void recv_msg(Bytes &payload);
};

struct IoResult
{
    enum Tag
    {
        Done,       // the read or write is complete
        Blocked,    // the operation blocked, but some progress may have been made
        Disconnect, // the connection was lost
    } tag;

    size_t n_bytes;
};

enum class WriteResult
{
    Done,       // the read or write is complete
    Blocked,    // the operation blocked, but some progress may have been made
    Disconnect, // the connection was lost
};

enum class ReadResult
{
    Read,       // data was read
    Blocked,    // we might have read some data, but the fd blocked
    Disconnect, // the connection was lost
    BadMagic,   // the magic didn't match
    BadType,    // invalid message type
};
