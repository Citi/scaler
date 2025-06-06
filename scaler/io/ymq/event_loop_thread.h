#pragma once

#include <map>
#include <thread>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/event_loop.h"
#include "scaler/io/ymq/typedefs.h"

class IOSocket;

class EventLoopThread {
    using PollingContext = configuration::polling_context_t;
    std::thread thread;
    std::map<std::string, IOSocket> identityToIOSocket;
    EventLoop<PollingContext> eventLoop;

public:
    // Why not make the class a friend class of IOContext?
    // Because the removeIOSocket method is a bit trickier than addIOSocket,
    // the IOSocket that is being removed will first remove every MessageConnectionTCP
    // managed by it from the EventLoop, before it removes it self from ioSockets.
    // return eventLoop.executeNow(createIOSocket());
    IOSocket* addIOSocket(std::string identity, IOSocketType socketType);

    bool removeIOSocket(IOSocket*);
    // EventLoop<PollingContext>& getEventLoop();
    // IOSocket* getIOSocketByIdentity(size_t identity);

    EventLoopThread(const EventLoopThread&)            = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;
    // TODO: Revisit the default ctor
    EventLoopThread() = default;
};
