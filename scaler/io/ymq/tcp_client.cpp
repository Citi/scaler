#include "scaler/io/ymq/tcp_client.h"

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"

void TcpClient::onCreated(std::string identity) {
    printf("tcpClient onAdded\n");
    this->_IOSocketIdentity = identity;
    // _eventLoopThread->eventLoop.addFdToLoop(_clientFd, EPOLLIN, this->_eventManager.get());
}

TcpClient::TcpClient(std::shared_ptr<EventLoopThread> eventLoopThread): _eventLoopThread(eventLoopThread) {
    _eventManager          = std::make_unique<EventManager>(_eventLoopThread);
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
    // _serverFd     = create_and_bind_socket();
}

void TcpClient::onRead() {
    // printf("TcpServer::onRead()\n");
    // int fd         = accept4(_serverFd, &_addr, &_addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    // std::string id = this->_IOSocketIdentity;
    // auto& sock     = this->_eventLoopThread->_identityToIOSocket.at(id);
    // // FIXME: the second _addr is not real
    // sock._fdToConnection[fd] = new MessageConnectionTCP(_eventLoopThread, fd, _addr, _addr);
    // sock._fdToConnection[fd]->onCreated();
}

TcpClient::~TcpClient() {}
