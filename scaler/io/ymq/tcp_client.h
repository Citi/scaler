#pragma once

// C++
#include <functional>
#include <memory>

// First-party
#include "scaler/io/ymq/file_descriptor.h"
// #include "event_manager.hpp"
// #include "scaler/io/ymq/event_loop_thread.h"

class EventLoopThread;
class EventManager;

class TcpClient {
    std::shared_ptr<EventLoopThread> _eventLoopThread; /* shared ownership */
    std::unique_ptr<EventManager> _eventManager;
    // Implementation defined method. connect(3) should happen here.
    // This function will call user defined onConnectReturn()
    // It will handle error it can handle. If it is unreasonable to
    // handle the error here, pass it to onConnectReturn()
    void onRead();
    void onWrite() {}
    void onClose() {}
    void onError() {}

public:
    std::string _IOSocketIdentity;
    TcpClient(const TcpClient&)            = delete;
    TcpClient& operator=(const TcpClient&) = delete;
    // TODO: Modify this behavior
    TcpClient(std::shared_ptr<EventLoopThread> eventLoopThread);

    using ConnectReturnCallback = std::function<void(FileDescriptor, sockaddr, int)>;
    ConnectReturnCallback onConnectReturn;

    void onCreated(std::string identity);

    void retry(/* Arguments */);
    ~TcpClient();
};
