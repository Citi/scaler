#include "scaler/io/ymq/io_socket.h"

#include "scaler/io/ymq/tcp_client.h"
#include "scaler/io/ymq/tcp_server.h"
// NOTE: We need it after we put impl
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/message_connection_tcp.h"

void IOSocket::onCreated() {
    // Detect if we need to initialize tcpClient and/or tcpServer
    // If so, initialize it, and then call their onAdd();
    if (_socketType == IOSocketType::Router) {
        // assert(!tcpClient);
        _tcpClient.emplace(_eventLoopThread);
        // assert(!tcpServer);
        _tcpServer.emplace(_eventLoopThread);
        _tcpClient->onCreated(this->identity());
        _tcpServer->onCreated(this->identity());
    }
    // Different SocketType might have different rules
}

IOSocket::IOSocket(std::shared_ptr<EventLoopThread> eventLoopThread, Identity identity, IOSocketType socketType)
    : _eventLoopThread(eventLoopThread), _identity(identity), _socketType(socketType) {}

IOSocket::IOSocket() {}

void IOSocket::sendMessage(const std::vector<char>& buf, std::function<void()> callback) {
    if (_socketType == IOSocketType::Router) {
        this->_eventLoopThread->_eventLoop.executeNow([] {
            // for (auto conn : ) {
            //
            // }
        });
    }
}

void IOSocket::recvMessage(std::vector<char>& buf) {}
