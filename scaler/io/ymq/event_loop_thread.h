#pragma once

// C++
#include <memory>
#include <map>
#include <string>
#include <thread>

// First-party
// #include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/event_loop.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/epoll_context.h"

class IOSocket;

class EventLoopThread {
    using PollingContext = EpollContext;
    using Identity       = std::string;

    std::thread thread;
    std::map<Identity, std::shared_ptr<IOSocket>> identityToIOSocket;
    EventLoop<PollingContext> eventLoop;

public:
    // Why not make the class a friend class of IOContext?
    // Because the removeIOSocket method is a bit trickier than addIOSocket,
    // the IOSocket that is being removed will first remove every MessageConnectionTCP
    // managed by it from the EventLoop, before it removes it self from ioSockets.
    // return eventLoop.executeNow(createIOSocket());
    void addIOSocket(std::shared_ptr<IOSocket>);
    bool removeIOSocket(std::shared_ptr<IOSocket>);
    std::shared_ptr<IOSocket> getIOSocketByIdentity(size_t identity);

    EventLoop<PollingContext>& getEventLoop() { return eventLoop; }

    // EventLoopThread(const EventLoopThread&)            = delete;
    // EventLoopThread& operator=(const EventLoopThread&) = delete;
};
