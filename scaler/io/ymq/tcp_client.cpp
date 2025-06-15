#include "scaler/io/ymq/tcp_client.h"

#include <sys/socket.h>

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/message_connection_tcp.h"

// TODO: This is certainly wrong, think about it
void TcpClient::onCreated(std::string identity) {
    this->_IOSocketIdentity = identity;
    // _eventLoopThread->eventLoop.addFdToLoop(_clientFd, EPOLLIN, this->_eventManager.get());
}

// TODO: Veryify if this correct
// - The tcpclient should be removed in loop so that we don't have memory leak
// - The tcpclient should consider delay execution in the loop
// - in general clean up this function
void TcpClient::onCreated(std::string identity, sockaddr addr) {
    this->_IOSocketIdentity = identity;

    int sockfd    = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    this->_connFd = sockfd;
    int ret       = connect(sockfd, (sockaddr*)&addr, sizeof(addr));
    if (ret < 0) {
        if (errno != EINPROGRESS) {
            perror("connect");
            close(sockfd);
            return;
        } else {
            // TODO: Think about the lifetime of _eventManager.
            _eventLoopThread->_eventLoop.addFdToLoop(sockfd, EPOLLOUT, this->_eventManager.get());
        }
    } else {
        // success
        printf("client SUCCESS\n");
        std::string id = this->_IOSocketIdentity;
        auto& sock     = this->_eventLoopThread->_identityToIOSocket.at(id);
        // FIXME: the second _addr is not real
        sock->_fdToConnection[sockfd] =
            std::make_unique<MessageConnectionTCP>(_eventLoopThread, sockfd, addr, addr, id);
        sock->_fdToConnection[sockfd]->onCreated();
        // The idea is, this tcpClient needs to be reset
    }
}

TcpClient::TcpClient(std::shared_ptr<EventLoopThread> eventLoopThread): _eventLoopThread(eventLoopThread) {
    _eventManager          = std::make_unique<EventManager>(_eventLoopThread);
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
    // _serverFd     = create_and_bind_socket();
}

void TcpClient::onWrite() {
    // assuming success
    sockaddr addr;
    int ret = connect(_connFd, (sockaddr*)&addr, sizeof(addr));
    // TODO: -^ what if this connect failed?
    std::string id = this->_IOSocketIdentity;
    auto& sock     = this->_eventLoopThread->_identityToIOSocket.at(id);
    // FIXME: the second _addr is not real
    sock->_fdToConnection[_connFd] = std::make_unique<MessageConnectionTCP>(_eventLoopThread, _connFd, addr, addr, id);
    sock->_fdToConnection[_connFd]->onCreated();
}

void TcpClient::onRead() {
    printf("TcpClient::onRead()\n");
}

TcpClient::~TcpClient() {}
