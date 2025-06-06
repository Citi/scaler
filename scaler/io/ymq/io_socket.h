#pragma once

// C++
// #include <map>
// #include <optional>
#include <memory>
#include <string>

// First-party
// #include "scaler/io/ymq/message_connection_tcp.hpp"
// #include "scaler/io/ymq/tcp_client.hpp"
// #include "scaler/io/ymq/tcp_server.hpp"

// #include "message_connection_tcp.hpp"
// #include "tcp_client.hpp"
// #include "tcp_server.hpp"

#include "scaler/io/ymq/typedefs.h"

class EventLoopThread;

class IOSocket {
    std::shared_ptr<EventLoopThread> eventLoopThread;

    // std::optional<TcpServer> tcpServer;
    // std::optional<TcpClient> tcpClient;
    // std::map<int /* class FileDescriptor */, MessageConnectionTCP*> fdToConnection;
    // std::map<std::string, MessageConnectionTCP*> identityToConnection;

public:
    IOSocket(const IOSocket&)            = delete;
    IOSocket& operator=(const IOSocket&) = delete;
    IOSocket(IOSocket&&)                 = delete;
    IOSocket& operator=(IOSocket&&)      = delete;

    const std::string identity;
    const IOSocketType socketTypes;
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

    // void recvMessage(Message* msg);
};
