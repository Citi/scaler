#pragma once

#include <memory>
#include <optional>
#include <tuple>

#include "scaler/io/ymq/file_descriptor.h"
#include "scaler/io/ymq/message_connection.h"

class EventLoopThread;
class EventManager;

class TcpWriteOperation {};
class TcpReadOperation {};

class MessageConnectionTCP: public MessageConnection {
    FileDescriptor _connFd;
    sockaddr _localAddr;
    sockaddr _remoteAddr;

    TcpWriteOperation write_op;
    TcpReadOperation read_op;

    std::shared_ptr<EventLoopThread> _eventLoopThread;
    std::unique_ptr<EventManager> _eventManager;

public:
    MessageConnectionTCP(
        std::shared_ptr<EventLoopThread> eventLoopThread,
        FileDescriptor connFd,
        sockaddr localAddr,
        sockaddr remoteAddr);

    void send(Bytes data, SendMessageContinuation k) { todo(); }
    void recv(RecvMessageContinuation k) { todo(); }

    void onCreated();
};
