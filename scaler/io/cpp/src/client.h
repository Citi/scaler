#pragma once

// C++
#include <optional>
#include <vector>

// System
#include <sys/socket.h>

// Third-party
#include "third_party/concurrentqueue.h"

// First-party
#include "common.h"
#include "session.h"

using moodycamel::ConcurrentQueue;

struct Peer
{
    Client *client;        // the binder that this peer belongs to
    Bytes identity;        // the peer's address, i.e. identity
    sockaddr_storage addr; // the peer's address, if we are the connector
    int fd;                // the tcp socket fd of this peer
};

struct SendMsg
{
    // the future to resolve when the message is sent
    void *future;

    // the message to send
    Message msg;
};

enum class ConnectorType{
    Pair,
    Pub,
    Sub,
    Dealer,
    Router // only valid for binder interface
};

// Clients are tcp or unix domain sockets (uds, ipc)
// no variant for in-process because they're handled separately
enum class Transport{
    TCP,
    IntraProcess,
    InterProcess};

struct Client
{
    ConnectorType type;
    Transport transport;

    Session *session; // backreference to session
    Bytes identity;   // the identity of this client

    int rr; // round robin for dealer

    int fd;                               // the bound socket, <0 when not bound
    std::optional<sockaddr_storage> addr; // addr for when we're bound
    std::vector<Peer *> peers;

    int unmuted_event_fd; // event fd for when the client is no longer muted

    int send_event_fd;             // event fd for send queue
    ConcurrentQueue<SendMsg> send; // the send queue for Python thread -> io thread communication

    int recv_event_fd;                    // event fd for recv queue
    ConcurrentQueue<void *> recv;         // the recv queue for io thread -> Python thread communication
    int recv_buffer_event_fd;             // event fd for recv buffer, only needed for sync clients
    ConcurrentQueue<Message> recv_buffer; // these are messages that have been received

    // must hold mutex
    bool peer_by_id(Bytes id, Peer **peer)
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

    inline bool muted()
    {
        // these types mute when they have no peers
        if (this->type == ConnectorType::Pair || this->type == ConnectorType::Dealer)
        {
            return this->peers.empty();
        }

        // other types drop messages when they have no peers
        return false;
    }

    void recv_msg(Message &&msg);
    void unmute();
};
