#include "scaler/io/ymq/tcp_server.h"

#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <memory>
#include <system_error>

#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/file_descriptor.h"

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

TcpServer::TcpServer(std::shared_ptr<EventLoopThread> eventLoop): _eventLoopThread(eventLoop) {
    auto fd = FileDescriptor::socket(AF_INET, SOCK_STREAM, 0);

    if (!fd) {
        throw std::system_error(fd.error(), std::system_category(), "Failed to create socket");
    }

    const sockaddr_in addr {
        .sin_family = AF_INET,
        .sin_port   = htons(8080),
        .sin_addr   = {.s_addr = INADDR_ANY},
    };

    if (auto err = fd->bind((const sockaddr&)addr, sizeof(addr)); err) {
        throw std::system_error(*err, std::system_category(), "Failed to bind socket");
    }

    if (auto err = fd->listen(SOMAXCONN); err) {
        throw std::system_error(*err, std::system_category(), "Failed to listen on socket");
    }

    _eventManager = std::make_unique<EventManager>(
        eventLoop, std::move(*fd), [this](FileDescriptor& fd, Events events) { this->onCreated(); });
}

void TcpServer::onCreated() {
    printf("TcpServer::onAdded()\n");
    _eventLoopThread->registerEventManager(*this->_eventManager.get());
}
