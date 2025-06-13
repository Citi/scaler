#include "scaler/io/ymq/tcp_server.h"

#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <memory>

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/message_connection_tcp.h"

static int create_and_bind_socket() {
    int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (server_fd == -1) {
        perror("socket");
        return -1;
    }

    sockaddr_in addr {};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(8080);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind");
        close(server_fd);
        return -1;
    }

    if (listen(server_fd, SOMAXCONN) == -1) {
        perror("listen");
        close(server_fd);
        return -1;
    }

    return server_fd;
}

// TODO: Allow user to specify port/addr
TcpServer::TcpServer(std::shared_ptr<EventLoopThread> eventLoopThread)
    : _eventLoopThread(eventLoopThread), _eventManager(std::make_unique<EventManager>(_eventLoopThread)) {
    _serverFd = create_and_bind_socket();

    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void TcpServer::onCreated(std::string identity) {
    printf("%s, %d\n", __PRETTY_FUNCTION__, __LINE__);
    // _eventLoopThread->eventLoop.registerEventManager(*this->_eventManager.get());
    // TODO: Think about this, maybe move this to ctor?
    this->_IOSocketIdentity = identity;
    _eventLoopThread->_eventLoop.addFdToLoop(_serverFd, EPOLLIN, this->_eventManager.get());
}

void TcpServer::onRead() {
    printf("TcpServer::onRead()\n");
    int fd = accept4(_serverFd, &_addr, &_addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    close(fd);
    return;
    // std::string id = this->_IOSocketIdentity;
    // auto& sock     = this->_eventLoopThread->_identityToIOSocket.at(id);
    // // FIXME: the second _addr is not real
    // sock->_fdToConnection[fd] = std::make_unique<MessageConnectionTCP>(_eventLoopThread, fd, _addr, _addr);
    // sock->_fdToConnection[fd]->onCreated();
}

TcpServer::~TcpServer() {}
