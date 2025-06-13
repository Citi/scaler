#pragma once

// C++
#include <sys/socket.h>

#include <functional>
#include <memory>

// First-party
#include "scaler/io/ymq/file_descriptor.h"
// #include "event_loop_thread.hpp"
// #include "event_manager.hpp"

class EventLoopThread;
class EventManager;

// struct sockaddr *__restrict addr, socklen_t *__restrict addr_len

class TcpServer {
    // eventLoop thread will call onRead that is associated w/ the eventManager
    std::shared_ptr<EventLoopThread> _eventLoopThread;
    std::unique_ptr<EventManager> _eventManager;  // will copy the `onRead()` to itself
    int _serverFd;
    // Implementation defined method. accept(3) should happen here.
    // This function will call user defined onAcceptReturn()
    // It will handle error it can handle. If it is unreasonable to
    // handle the error here, pass it to onAcceptReturn()
    void onRead();
    void onWrite() {}
    void onClose() {}
    void onError() {}

    sockaddr _addr;
    socklen_t _addr_len;
    std::string _IOSocketIdentity;

public:
    TcpServer(const TcpServer&)            = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // TODO: Modify the behavior of default ctor
    TcpServer(std::shared_ptr<EventLoopThread> eventLoop);

    using AcceptReturnCallback = std::function<void(FileDescriptor, sockaddr, int)>;
    AcceptReturnCallback onAcceptReturn;
    void onCreated(std::string identity);
    ~TcpServer();
};
