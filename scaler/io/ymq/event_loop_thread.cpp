
#include "scaler/io/ymq/event_loop_thread.h"

#include <cassert>
#include <memory>

#include "scaler/io/ymq/common.h"
#include "scaler/io/ymq/io_socket.h"

void EventLoopThread::addIOSocket(std::shared_ptr<IOSocket> socket) {
    _identityToIOSocket.emplace(socket->identity(), socket);

    todo();
}

// TODO: Think about non null pointer
void EventLoopThread::removeIOSocket(std::shared_ptr<IOSocket> socket) {
    // TODO: Something happen with the running thread
    _identityToIOSocket.erase(socket->identity());

    todo();
}

void EventLoopThread::registerEventManager(EventManager& em) {
    _eventLoop->registerEventManager(em);
}
void EventLoopThread::removeEventManager(EventManager& em) {
    // todo
}
