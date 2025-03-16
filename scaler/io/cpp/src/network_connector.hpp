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

struct NetworkConnector;
struct RawPeer;
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

void write_to_peer(RawPeer *peer, SendMessage send);
void reconnect_peer(RawPeer *peer);
void remove_peer(RawPeer *peer);
ControlFlow epollin_peer(RawPeer *peer);
ControlFlow epollout_peer(RawPeer *peer);

void network_connector_bind_tcp(NetworkConnector *connector, const char *host, uint16_t port);
void network_connector_bind_unix(NetworkConnector *connector, const char *path);

// -- interface --

void network_connector_init(Session *session, NetworkConnector *connector, Transport transport, ConnectorType type, uint8_t *identity, size_t len);
void network_connector_bind(NetworkConnector *connector, const char *host, uint16_t port);
void network_connector_connect(NetworkConnector *connector, const char *addr, uint16_t port);
void network_connector_send(void *future, NetworkConnector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void network_connector_send_sync(NetworkConnector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void network_connector_recv(void *future, NetworkConnector *connector);
void network_connector_recv_sync(NetworkConnector *connector, Message *msg);
void network_connector_destroy(NetworkConnector *connector);

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

struct NetworkConnector
{
    ConnectorType type;
    Transport transport;

    ThreadContext *thread; // the thread that this client is bound to
    Session *session;      // backreference to session
    Bytes identity;        // the identity of this client

    size_t rr; // round robin for dealer

    int fd;                               // the bound socket, <0 when not bound
    std::optional<sockaddr_storage> addr; // addr for when we're bound
    std::vector<RawPeer *> peers;

    int send_event_fd;                       // event fd for send queue
    ConcurrentQueue<SendMessage> send_queue; // the send queue for Python thread -> io thread communication
    int recv_event_fd;                       // event fd for recv queue
    ConcurrentQueue<void *> recv_queue;      // the recv queue for io thread -> Python thread communication
    int recv_buffer_event_fd;                // event fd for recv buffer, only needed for sync clients
    ConcurrentQueue<Message> recv_buffer;    // these are messages that have been received

    int destroy_tfd;
    std::optional<Completer> destroy; // this is used to wait for the destruction of a client

    // must hold mutex
    bool peer_by_id(Bytes id, RawPeer **peer);
    void remove_peer(RawPeer *peer);
    bool muted();
    size_t peer_rr();
    void recv_msg(Message message);
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

struct RawPeer
{
    NetworkConnector *connector; // the binder that this peer belongs to
    Bytes identity;              // the peer's address, i.e. identity
    sockaddr_storage addr;       // the peer's address
    PeerType type;               // the type of peer
    int fd;                      // the socket fd of this peer

    std::queue<SendMessage> queue; // messages to be sent by this peer

    PeerState state; // the state of the peer

    std::optional<IoOperation> read_op;  // the current read operation
    std::optional<IoOperation> write_op; // the current write operation

    void recv_msg(Bytes payload);
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
