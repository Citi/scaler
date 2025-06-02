#pragma once

// C++
#include <map>
#include <optional>
#include <string>

// First-party
#include "event_loop_thread.hpp"
#include "message_connection_tcp.hpp"
#include "scaler/io/ymq/file_descriptor.hpp"
#include "tcp_client.hpp"
#include "tcp_server.hpp"

enum class SocketTypes { Binder, Sub, Pub, Dealer, Router, Pair };

class IOSocket {
    EventLoopThread& eventLoopThread;
    SocketTypes socketType;

    std::optional<TcpServer> tcpServer;
    std::optional<TcpClient> tcpClient;
    std::map<FileDescriptor, MessageConnectionTCP*> fdToConnection;
    std::map<std::string, MessageConnectionTCP*> identityToConnection;

public:
    IOSocket(EventLoopThread& eventLoopThread, const std::string& identity, SocketTypes socketType)
        : eventLoopThread(eventLoopThread), identity(identity), socketType(socketType) {}

    IOSocket(const IOSocket&)            = delete;
    IOSocket& operator=(const IOSocket&) = delete;
    IOSocket(IOSocket&&)                 = delete;
    IOSocket& operator=(IOSocket&&)      = delete;

    const std::string identity;
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
