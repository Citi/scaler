
#include "scaler/io/ymq/message_connection_tcp.h"

#include <unistd.h>

#include <memory>

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"

// TODO: Accepts localSocketIdentity from caller,
// wait until the network is established and get remoteSocketIdentity
MessageConnectionTCP::MessageConnectionTCP(
    std::shared_ptr<EventLoopThread> eventLoopThread, int connFd, sockaddr localAddr, sockaddr remoteAddr)
    : _eventLoopThread(eventLoopThread)
    , _eventManager(std::make_unique<EventManager>(_eventLoopThread))
    , _connFd(std::move(connFd))
    , _localAddr(localAddr)
    , _remoteAddr(remoteAddr) {
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void MessageConnectionTCP::onCreated() {
    this->_eventLoopThread->_eventLoop.addFdToLoop(_connFd, EPOLLIN | EPOLLOUT | EPOLLET, this->_eventManager.get());
}

void MessageConnectionTCP::onRead() {
    // Obviously, do more error handling.
    // Find operation, if presents, do callback
    if (!_recvBuf.size())
        _recvBuf.resize(1024);
    if (_readOp) {
        int n = read(this->_connFd, _recvBuf.data() + _readCursor, 1024);
        _readCursor += n;
        _readOp->_libCallback(_recvBuf, _readCursor);
    }
}

void MessageConnectionTCP::onWrite() {
    // Get the current operation
    // If presents, do write those, if completes, do callback
    if (_writeOp) {
        // A loop until EAGAIN
        _writeOp->_writeCursor += write(
            this->_connFd,
            _writeOp->_buf.data() + _writeOp->_writeCursor,
            _writeOp->_buf.size() - _writeOp->_writeCursor);
        _writeOp->_callback();
    }
}

MessageConnectionTCP::~MessageConnectionTCP() {}
