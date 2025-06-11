#include "scaler/io/ymq/tcp_server.h"

#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <memory>

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
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

TcpServer::TcpServer(std::shared_ptr<EventLoopThread> eventLoopThread): _eventLoopThread(eventLoopThread) {
    _eventManager = std::make_unique<EventManager>(EventManager());
    _serverFd     = create_and_bind_socket();
}

void TcpServer::onCreated() {
    printf("TcpServer::onAdded()\n");
    eventLoopThread->eventLoop.registerEventManager(*this->eventManager.get());
    // _eventLoopThread->eventLoop.addFdToLoop(_serverFd, EPOLLIN, this->_eventManager.get());
}

void TcpServer::onRead() {
    // printf("TcpServer::onRead()\n");
    // int fd = accept4(_serverFd, &_addr, &_addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    // // MessageConnectionTCP newConn(fd, addr, );
    // // TODO: Handle accept4 error
    // _eventLoopThread->eventLoop.addFdToLoop(_serverFd, EPOLLIN | EPOLLOUT | EPOLLET, this->_eventManager.get());
    // // put into eventloop
}
