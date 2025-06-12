#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <tuple>
#include <vector>

#include "scaler/io/ymq/file_descriptor.h"
#include "scaler/io/ymq/message_connection.h"

class EventLoopThread;
class EventManager;

struct TcpWriteOperation {
    std::function<void()> _callback;
    std::function<void(std::vector<char>&, size_t)> _libCallback;
    std::vector<char> _buf;
    size_t _writeCursor;
};

struct TcpReadOperation {
    std::function<void()> _callback;
    std::function<void(std::vector<char>&, size_t)> _libCallback;
    std::vector<char> _buf;
};

class MessageConnectionTCP: public MessageConnection {
    int _connFd;
    sockaddr _localAddr;
    sockaddr _remoteAddr;
    std::string _localIOSocketIdentity;
    std::string _remoteIOSocketIdentity;

    std::vector<char> _recvBuf;
    size_t _readCursor = 0;

    std::optional<TcpWriteOperation> _writeOp;
    std::optional<TcpReadOperation> _readOp;

    std::shared_ptr<EventLoopThread> _eventLoopThread;
    std::unique_ptr<EventManager> _eventManager;

    void onRead();
    void onWrite();
    void onClose() {};
    void onError() {};

public:
    ~MessageConnectionTCP();
    MessageConnectionTCP(
        std::shared_ptr<EventLoopThread> eventLoopThread, int connFd, sockaddr localAddr, sockaddr remoteAddr);

    void send(Bytes data, SendMessageContinuation k) { todo(); }
    void recv(RecvMessageContinuation k) { todo(); }

    void onCreated();
};
