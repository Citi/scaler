
#include "scaler/io/ymq/message_connection_tcp.h"

#include <memory>

#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/file_descriptor.h"

MessageConnectionTCP::MessageConnectionTCP(
    std::shared_ptr<EventLoopThread> eventLoopThread, FileDescriptor connFd, sockaddr localAddr, sockaddr remoteAddr)
    : _eventLoopThread(eventLoopThread), _connFd(std::move(connFd)), _localAddr(localAddr), _remoteAddr(remoteAddr) {
    _eventManager = std::make_unique<EventManager>();
}
