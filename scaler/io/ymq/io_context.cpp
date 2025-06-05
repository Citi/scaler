#include "event_loop_thread.h"
#include "io_context.h"

std::shared_ptr<IOSocket> IOContext::createIOSocket(Identity identity, SocketTypes socketType) {
    static size_t threadsRoundRobin = 0;
    auto& thread = _threads[threadsRoundRobin];
    ++threadsRoundRobin %= _threads.size();

    auto socket = std::make_shared<IOSocket>(thread, identity, socketType);
    thread.addIOSocket(socket);
    return socket;
}

bool IOContext::removeIOSocket(std::shared_ptr<IOSocket> socket) {
    todo();
}
