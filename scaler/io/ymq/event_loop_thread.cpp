
#include "scaler/io/ymq/event_loop_thread.h"

#include <cassert>
#include <memory>

#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/io_socket.h"

std::shared_ptr<IOSocket> EventLoopThread::createIOSocket(std::string identity, IOSocketType socketType) {
    if (thread.get_id() == std::thread::id()) {
        thread = std::jthread([this](std::stop_token token) {
            // while (!token.stop_requested()) {
            while (true) {
                printf("A loop starts\n");
                sleep(10);
                this->_eventLoop.loop();
            }
        });
    }

    auto [iterator, inserted] =
        _identityToIOSocket.try_emplace(identity, std::make_shared<IOSocket>(shared_from_this(), identity, socketType));
    assert(inserted);
    auto ptr = iterator->second;

    _eventLoop.executeNow([ptr] { ptr->onCreated(); });
    return ptr;
}

// TODO: Think about non null pointer
void EventLoopThread::removeIOSocket(IOSocket* target) {
    // TODO: Something happen with the running thread
    _identityToIOSocket.erase(target->identity());
}
