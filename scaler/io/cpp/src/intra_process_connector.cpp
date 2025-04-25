#include "intra_process_connector.hpp"

Status IntraProcessConnector::ensure_epoll() {
    if (this->thread == nullptr)
        return Status::from_code("async operations require a session with threads", Code::NoThreads);

    if (this->epoll.exchange(true))
        return Status::ok();

    this->thread->add_epoll(this->recv_buffer_event_fd, EPOLLIN | EPOLLET, EpollType::IntraProcessConnectorRecv, this);
    this->thread->add_epoll(this->recv_event_fd, EPOLLIN | EPOLLET, EpollType::IntraProcessConnectorRecv, this);

    return Status::ok();
}

void IntraProcessConnector::remove_from_epoll() {
    if (!this->epoll)
        return;

    this->thread->remove_epoll(this->recv_buffer_event_fd);
    this->thread->remove_epoll(this->recv_event_fd);
}

Status intra_process_init(Session* session, IntraProcessConnector* connector, uint8_t* identity, size_t len) {
    new (connector) IntraProcessConnector {
        .session              = session,
        .thread               = session->next_thread(),
        .queue                = ConcurrentQueue<Message>(),
        .recv                 = ConcurrentQueue<void*>(),
        .recv_buffer_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .recv_event_fd        = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .unmuted_event_fd     = eventfd(0, EFD_NONBLOCK),
        .identity             = Bytes::copy(identity, len),
        .bind                 = std::nullopt,
        .connecting           = std::nullopt,
        .peer                 = std::nullopt,
        .epoll                = false,
    };

    // take exclusive lock on the session to add the client
    session->intra_process_mutex.lock();
    session->inprocs.push_back(connector);
    session->intra_process_mutex.unlock();

    return Status::ok();
}

Status intra_process_bind(IntraProcessConnector* connector, const char* addr) {
    if (connector->bind)
        return Status::from_code("interprocess connector already bound", Code::AlreadyBound);

    std::string bind(addr);

    connector->session->intra_process_mutex.lock();

    // check for conflicts
    for (auto other : connector->session->inprocs) {
        if (other == connector)
            continue;

        if (other->bind == bind)
            return Status::from_errno("intraprocess address already in use", EADDRINUSE);
    }

    // set the bind address
    connector->bind = bind;

    // check for any pending connections
    for (auto other : connector->session->inprocs) {
        if (other == connector)
            continue;

        if (other->connecting == bind) {
            other->connecting = std::nullopt;
            other->peer       = connector;
            connector->peer   = other;

            if (eventfd_signal(other->unmuted_event_fd) < 0)
                return Status::from_errno("intraprocess failed to signal unmuted_event_fd");
        }
    }

    connector->session->intra_process_mutex.unlock();

    return Status::ok();
}

Status intra_process_connect(IntraProcessConnector* connector, const char* addr) {
    std::string connecting(addr);

    connector->session->intra_process_mutex.lock();

    for (auto other : connector->session->inprocs) {
        if (other == connector)
            continue;

        // we found a matching bind
        if (other->bind == connecting) {
            other->peer     = connector;
            connector->peer = other;

            if (eventfd_signal(other->unmuted_event_fd) < 0)
                return Status::from_errno("failed to signal unmuted_event_fd");

            connector->session->intra_process_mutex.unlock();
            return Status::ok();
        }
    }

    // the connection is pending
    connector->connecting = connecting;
    connector->session->intra_process_mutex.unlock();

    return Status::ok();
}

Status intra_process_send(IntraProcessConnector* connector, uint8_t* data, size_t len) {
    for (;;) {
        connector->session->intra_process_mutex.lock_shared();

        if (connector->peer) {
            Message msg {
                // we need to clone the identity because the sending client
                // has an independent lifetime from the message / receiving client
                .address = Bytes::clone(connector->identity),

                // the caller (Python) owns the data, so we need to copy it
                .payload = Bytes::copy(data, len),
            };

            auto peer = *connector->peer;
            peer->queue.enqueue(msg);

            // signal the receiving client (semaphore)
            if (eventfd_signal(peer->recv_buffer_event_fd) < 0)
                return Status::from_errno("failed to signal recv_buffer_event_fd");

            connector->session->intra_process_mutex.unlock_shared();
            return Status::ok();
        }

        connector->session->intra_process_mutex.unlock_shared();

    // wait for a connection
    wait:
        if (auto code = fd_wait(connector->unmuted_event_fd, -1, POLLIN)) {
            if (code > 0)
                return Status::from_signal("fdwait: unmuted_event_fd", code);

            return Status::from_errno("failed to wait for unmuted_event_fd");
        }

        if (eventfd_wait(connector->unmuted_event_fd) < 0) {
            // pre-empted, go back to waiting
            if (errno == EAGAIN)
                goto wait;

            return Status::from_errno("failed to wait on unmuted_event_fd");
        }
    }

    return Status::ok();
}

Status intra_process_recv_sync(IntraProcessConnector* connector, Message* msg) {
wait:
    if (auto code = fd_wait(connector->recv_buffer_event_fd, -1, POLLIN)) {
        if (code > 0)
            return Status::from_signal("fdwait: recv_buffer_event_fd", code);

        return Status::from_errno("failed to wait for recv_buffer_event_fd");
    }

    if (eventfd_wait(connector->recv_buffer_event_fd) < 0) {
        if (errno == EAGAIN)
            goto wait;  // pre-empted, try again

        return Status::from_errno("failed to read eventfd");
    }

    // after decrementing the semaphore, we have claimed the message from the queue
    // this also guarantees that the message is in the queue
    while (!connector->queue.try_dequeue(*msg))
        ;  // wait

    return Status::ok();
}

void intra_process_recv_async(void* future, IntraProcessConnector* connector) {
    // ensure that the client is in the epoll
    // this allows sync-only clients to avoid epoll overhead
    if (auto status = connector->ensure_epoll(); status.type != ErrorType::Ok)
        return future_set_status(future, &status);

    connector->recv.enqueue(future);

    if (eventfd_signal(connector->recv_event_fd) < 0) {
        auto status = Status::from_errno("failed to signal recv_event_fd");
        return future_set_status(future, &status);
    }
}

Status intra_process_destroy(IntraProcessConnector* connector) {
    connector->remove_from_epoll();
    connector->session->intra_process_mutex.lock();
    std::erase(connector->session->inprocs, connector);

    if (connector->peer) {
        auto peer  = *connector->peer;
        peer->peer = std::nullopt;
    }

    connector->session->intra_process_mutex.unlock();
    connector->identity.free();

    return Status::ok();
}
