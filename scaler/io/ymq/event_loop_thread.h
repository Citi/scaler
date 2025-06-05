#pragma once

// C++
#include <map>
#include <thread>

// First-party
#include "configuration.h"
#include "event_loop.h"
// #include "io_socket.hpp"

class IOSocket;

class EventLoopThread {
    using PollingContext = configuration::polling_context_t;
    using Identity = configuration::Identity;

    std::thread thread;
    std::map<Identity, IOSocket> identityToIOSocket;
    EventLoop<PollingContext> eventLoop;

public:
    // Why not make the class a friend class of IOContext?
    // Because the removeIOSocket method is a bit trickier than addIOSocket,
    // the IOSocket that is being removed will first remove every MessageConnectionTCP
    // managed by it from the EventLoop, before it removes it self from ioSockets.
    // return eventLoop.executeNow(createIOSocket());
    IOSocket* addIOSocket(std::string identity, std::string socketType);

    bool removeIOSocket(IOSocket*);
    // EventLoop<PollingContext>& getEventLoop();
    // IOSocket* getIOSocketByIdentity(size_t identity);

    // EventLoopThread(const EventLoopThread&)            = delete;
    // EventLoopThread& operator=(const EventLoopThread&) = delete;
};
