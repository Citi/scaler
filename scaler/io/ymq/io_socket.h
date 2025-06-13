#pragma once

// C++
#include <map>
#include <memory>
#include <optional>

// First-party
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/message_connection_tcp.h"
#include "scaler/io/ymq/tcp_client.h"
#include "scaler/io/ymq/tcp_server.h"
#include "scaler/io/ymq/typedefs.h"

// NOTE: Don't do this. It pollutes the env. I tried to remove it, but it reports err
// in pymod module. Consider include the corresponding file and define types there. - gxu
using Identity = Configuration::Identity;

class EventLoopThread;
class MessageConnectionTCP;

class IOSocket {
    std::shared_ptr<EventLoopThread> _eventLoopThread;
    Identity _identity;
    IOSocketType _socketType;

    std::optional<TcpClient> _tcpClient;
    std::optional<TcpServer> _tcpServer;
    // TODO: Figure out what this should do
    std::map<std::string, MessageConnectionTCP*> _identityToConnection;

public:
    std::map<int /* class FileDescriptor */, std::unique_ptr<MessageConnectionTCP>> _fdToConnection;

    IOSocket(std::shared_ptr<EventLoopThread> eventLoopThread, Identity identity, IOSocketType socketType);

    // TODO: Figure out what these should do
    IOSocket();
    IOSocket(const IOSocket&) {};
    IOSocket& operator=(const IOSocket&) { return *this; };
    // IOSocket(IOSocket&&)                 = delete;
    // IOSocket& operator=(IOSocket&&)      = delete;

    Identity identity() const { return _identity; }
    IOSocketType socketType() const { return _socketType; }

    // TODO: In the future, this will be Message
    void sendMessage(const std::vector<char>& buf, std::function<void()> callback, std::string remoteIdentity);
    void recvMessage(std::vector<char>& buf);

    void sendMessage(
        std::shared_ptr<std::vector<char>> buf, std::function<void()> callback, std::string remoteIdentity);

    // string -> connection mapping
    // and connection->string mapping

    // put it into the concurrent q, which is execute_now
    // void sendMessage(Message* msg, Continuation cont) {
    // EXAMPLE
    // execute_now(
    // switch (socketTypes) {
    //     case Pub:
    //         for (auto [fd, conn] &: fd_to_conn) {
    //             conn.send(msg.len, msg.size);
    //             conn.setWriteCompleteCallback(cont);
    //             eventLoopThread.getEventLoop().update_events(turn write on for this fd);
    //         }
    //         break;
    // }
    // )
    // }

    void onCreated();
    // TODO: Think about what the destructor should do
    ~IOSocket() {}

    // void recvMessage(Message* msg);
};
