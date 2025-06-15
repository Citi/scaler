
#include "scaler/io/ymq/message_connection_tcp.h"

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <memory>

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"

// TODO: Accepts localSocketIdentity from caller,
// wait until the network is established and get remoteSocketIdentity
// - Think about whether we need to have a localaddr/remoteaddr stuff
// Think about ctor
MessageConnectionTCP::MessageConnectionTCP(
    std::shared_ptr<EventLoopThread> eventLoopThread,
    int connFd,
    sockaddr localAddr,
    sockaddr remoteAddr,
    std::string localIOSocketIdentity)
    : _eventLoopThread(eventLoopThread)
    , _eventManager(std::make_unique<EventManager>(_eventLoopThread))
    , _connFd(std::move(connFd))
    , _localAddr(localAddr)
    , _remoteAddr(remoteAddr)
    , _localIOSocketIdentity(std::move(localIOSocketIdentity)) {
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void MessageConnectionTCP::onCreated() {
    this->_eventLoopThread->_eventLoop.addFdToLoop(_connFd, EPOLLIN | EPOLLOUT | EPOLLET, this->_eventManager.get());
}

void MessageConnectionTCP::onRead() {
    // assuming 64bit size + identity <= 128 bytes
    // TODO: Modernize
    // - And do not assume this 128bytes
    if (!_remoteIOSocketIdentity) {
        // idBuf needs to be aligned with 8 bytes boundary because of strict aliasing
        char idBuf[128] {};
        int n         = read(_connFd, &idBuf, 128);
        uint64_t size = *(uint64_t*)idBuf;
        char* first   = idBuf + sizeof(uint64_t);
        std::string remoteID(first, first + size);
        _remoteIOSocketIdentity.emplace(std::move(remoteID));
    }
}

void MessageConnectionTCP::onWrite() {
    // assuming 64bit size + identity <= 128 bytes
    // TODO: Modernize
    // - And do not assume this 128bytes
    // - remove log
    if (!_sendLocalIdentity) {
        // idBuf needs to be aligned with 8 bytes boundary because of strict aliasing
        char idBuf[128] {};
        *(uint64_t*)idBuf  = _localIOSocketIdentity.size();
        auto identityBegin = idBuf + sizeof(uint64_t);
        auto [_, last]     = std::ranges::copy(_localIOSocketIdentity, identityBegin);
        write(_connFd, idBuf, std::distance(idBuf, last));
        _sendLocalIdentity = true;
    }
}

MessageConnectionTCP::~MessageConnectionTCP() {}
