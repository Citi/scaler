#pragma once

// C++
#include <map>
#include <thread>

// First-party
#include "configuration.hpp"
#include "event_loop.hpp"
#include "io_socket.hpp"

struct IOSocket;

class EventLoopThread {
    using PollingContext = configuration::polling_context_t;
    using Identity = std::string;
    std::thread thread;
    std::map<Identity, IOSocket> identityToIOSocket;
    EventLoop<PollingContext> eventLoop;

public:
    // Why not make the class a friend class of IOContext?
    // Because the removeIOSocket method is a bit trickier than addIOSocket,
    // the IOSocket that is being removed will first remove every MessageConnectionTCP
    // managed by it from the EventLoop, before it removes it self from ioSockets.
    IOSocket* addIOSocket(/* args */);
    bool removeIOSocket(IOSocket*);
    EventLoop<PollingContext>& getEventLoop();
    IOSocket* getIOSocketByIdentity(size_t identity);

    EventLoopThread(const EventLoopThread&)            = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;
};
