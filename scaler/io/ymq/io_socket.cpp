#include "scaler/io/ymq/io_socket.h"

// NOTE: We need it after we put impl
#include "scaler/io/ymq/event_loop_thread.h"

void IOSocket::onAdded() {
    // Detect if we need to initialize tcpClient and/or tcpServer
    // If so, initialize it, and then call their onAdd();
    if (socketType == IOSocketType::Router) {
        // assert(!tcpClient);
        tcpClient.emplace(eventLoopThread);
        // assert(!tcpServer);
        tcpServer.emplace(eventLoopThread);
    }
    // tcpClient.onAdd();
    // tcpServer.onAdd();
}
