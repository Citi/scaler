#include "scaler/io/ymq/io_socket.h"

#include <memory>
#include <vector>

#include "scaler/io/ymq/tcp_client.h"
#include "scaler/io/ymq/tcp_server.h"
// NOTE: We need it after we put impl
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/message_connection_tcp.h"

// TODO: Think about this function
void IOSocket::onCreated() {
    // Different SocketType might have different rules
    if (_socketType == IOSocketType::Dealer) {
        _tcpServer.emplace(_eventLoopThread);
        _tcpServer->onCreated(this->identity());
    }
}

IOSocket::IOSocket(std::shared_ptr<EventLoopThread> eventLoopThread, Identity identity, IOSocketType socketType)
    : _eventLoopThread(eventLoopThread), _identity(identity), _socketType(socketType) {}

IOSocket::IOSocket() {}

void IOSocket::sendMessage(
    std::shared_ptr<std::vector<char>> buf, std::function<void()> callback, std::string remoteIdentity) {
    if (_socketType == IOSocketType::Router) {
        this->_eventLoopThread->_eventLoop.executeNow([this, buf, remoteIdentity] {
            auto connection = this->_identityToConnection[remoteIdentity];
            connection->send(buf);
        });
    }
}

void IOSocket::recvMessage(std::vector<char>& buf) {}

// TODO: This only pass in sockaddr is certainly not correct.
void IOSocket::connectTo(sockaddr addr) {
    _eventLoopThread->_eventLoop.executeNow([this, addr] {
        _tcpClient.emplace(_eventLoopThread);
        _tcpClient->onCreated(this->identity(), addr);
    });
}
