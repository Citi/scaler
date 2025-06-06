#pragma once

// C++
// #include <map>
// #include <optional>
#include <map>
#include <memory>
#include <string>

// First-party
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/file_descriptor.h"
#include "scaler/io/ymq/message_connection_tcp.h"
#include "scaler/io/ymq/tcp_client.h"
#include "scaler/io/ymq/tcp_server.h"

using Identity = configuration::Identity;

class TCPClient;
class TCPServer;

enum class SocketTypes { Binder, Sub, Pub, Dealer, Router, Pair };

class IOSocket {
    EventLoopThread& eventLoopThread;
    SocketTypes socketType;
    Identity identity;

    TCPServer* tcpServer;
    TCPClient* tcpClient;

    std::map<FileDescriptor, std::shared_ptr<MessageConnectionTCP>> fdToConnection;

public:
    IOSocket(EventLoopThread& eventLoopThread, Identity identity, SocketTypes socketType)
        : eventLoopThread(eventLoopThread), identity(identity), socketType(socketType) {}

    IOSocket(const IOSocket&)            = delete;
    IOSocket& operator=(const IOSocket&) = delete;
    IOSocket(IOSocket&&)                 = delete;
    IOSocket& operator=(IOSocket&&)      = delete;
};
