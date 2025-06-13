
#include "scaler/io/ymq/io_context.h"

#include <algorithm>  // std::ranges::generate
#include <cassert>    // assert
#include <memory>     // std::make_shared

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/typedefs.h"

IOContext::IOContext(size_t threadCount): _threads(threadCount) {
    assert(threadCount > 0);
    std::ranges::generate(_threads, std::make_shared<EventLoopThread>);
}

std::shared_ptr<IOSocket> IOContext::createIOSocket(Identity identity, IOSocketType socketType) {
    static size_t threadsRoundRobin = 0;
    auto& thread                    = _threads[threadsRoundRobin];
    ++threadsRoundRobin %= _threads.size();
    return thread->createIOSocket(identity, socketType);
}

bool IOContext::removeIOSocket(std::shared_ptr<IOSocket> socket) {
    return false;  // todo: implement this
}
