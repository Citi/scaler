#include "intra_process_connector.hpp"

void IntraProcessConnector::ensure_epoll()
{
    if (this->epoll.exchange(true))
        return;

    this->thread->add_epoll(this->recv_buffer_event_fd, EPOLLIN | EPOLLET, EpollType::IntraProcessConnectorRecv, this);
    this->thread->add_epoll(this->recv_event_fd, EPOLLIN | EPOLLET, EpollType::IntraProcessConnectorRecv, this);
}

void IntraProcessConnector::remove_from_epoll()
{
    if (!this->epoll)
        return;

    this->thread->remove_epoll(this->recv_buffer_event_fd);
    this->thread->remove_epoll(this->recv_event_fd);
}

void intra_process_init(Session *session, IntraProcessConnector *connector, uint8_t *identity, size_t len)
{
    new (connector) IntraProcessConnector{
        .session = session,
        .thread = session->next_thread(),
        .queue = ConcurrentQueue<Message>(),
        .recv = ConcurrentQueue<void *>(),
        .recv_buffer_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .recv_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .unmuted_event_fd = eventfd(0, EFD_NONBLOCK),
        .identity = Bytes::copy(identity, len),
        .bind = std::nullopt,
        .connecting = std::nullopt,
        .peer = std::nullopt,
        .epoll = false,
    };

    // take exclusive lock on the session to add the client
    session->intra_process_mutex.lock();
    session->inprocs.push_back(connector);
    session->intra_process_mutex.unlock();
}

void intra_process_bind(IntraProcessConnector *connector, const char *addr)
{
    if (connector->bind)
        panic("connector already bound");

    std::string bind(addr);

    connector->session->intra_process_mutex.lock();

    // check for conflicts
    for (size_t i = 0; i < connector->session->inprocs.size(); i++)
    {
        auto other = connector->session->inprocs[i];

        if (other == connector)
            continue;

        if (other->bind == bind)
            panic("intra_process_bind(): address already in use");
    }

    // set the bind address
    connector->bind = bind;

    // check for any pending connections
    for (size_t i = 0; i < connector->session->inprocs.size(); i++)
    {
        auto other = connector->session->inprocs[i];

        if (other == connector)
            continue;

        if (other->connecting == bind)
        {
            other->connecting = std::nullopt;
            other->peer = connector;
            connector->peer = other;

            if (eventfd_signal(other->unmuted_event_fd) < 0)
                panic("intra_process_bind(): failed to signal unmuted_event_fd");
        }
    }

    connector->session->intra_process_mutex.unlock();
}

void intra_process_connect(IntraProcessConnector *connector, const char *addr)
{
    std::string connecting(addr);

    connector->session->intra_process_mutex.lock();

    for (size_t i = 0; i < connector->session->inprocs.size(); i++)
    {
        auto other = connector->session->inprocs[i];

        if (other == connector)
            continue;

        // we found a matching bind
        if (other->bind == connecting)
        {
            other->peer = connector;
            connector->peer = other;

            if (eventfd_signal(other->unmuted_event_fd) < 0)
                panic("intra_process_connect(): failed to signal unmuted_event_fd");

            connector->session->intra_process_mutex.unlock();
            return;
        }
    }

    // the connection is pending
    connector->connecting = connecting;
    connector->session->intra_process_mutex.unlock();
}

void intra_process_send(IntraProcessConnector *connector, uint8_t *data, size_t len)
{
    for (;;)
    {
        connector->session->intra_process_mutex.lock_shared();

        if (connector->peer)
        {
            Message msg{
                .type = MessageType::Data,

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
                panic("intra_process_send(): failed to signal recv_buffer_event_fd");

            connector->session->intra_process_mutex.unlock_shared();
            return;
        }

        connector->session->intra_process_mutex.unlock_shared();

    // wait for a connection
    wait:
        if (auto code = fd_wait(connector->unmuted_event_fd, -1, POLLIN))
            panic("fd_wait(): " + std::to_string(code) + " ; " + std::to_string(errno));

        if (eventfd_wait(connector->unmuted_event_fd) < 0)
        {
            // pre-empted, go back to waiting
            if (errno == EAGAIN)
                goto wait;

            panic("intra_process_send(): failed to wait on unmuted_event_fd");
        }
    }
}

void intra_process_recv_sync(IntraProcessConnector *connector, Message *msg)
{
wait:
    if (auto code = fd_wait(connector->recv_buffer_event_fd, -1, POLLIN))
        panic("fd_wait(): " + std::to_string(code) + " ; " + std::to_string(errno));

    if (eventfd_wait(connector->recv_buffer_event_fd) < 0)
    {
        if (errno == EAGAIN)
            goto wait; // pre-empted, try again

        panic("intra_process_recv_sync(): failed to wait on recv_buffer_event_fd");
    }

    // after decrementing the semaphore, we have claimed the message from the queue
    // this also guarantees that the message is in the queue
    while (!connector->queue.try_dequeue(*msg))
        ; // wait
}

void intra_process_recv_async(void *future, IntraProcessConnector *connector)
{
    // ensure that the client is in the epoll
    // this allows sync-only clients to avoid epoll overhead
    connector->ensure_epoll();
    connector->recv.enqueue(future);

    if (eventfd_signal(connector->recv_event_fd) < 0)
        panic("intra_process_recv_async(): failed to signal recv_event_fd");
}

void intra_process_destroy(IntraProcessConnector *connector)
{
    connector->remove_from_epoll();
    connector->session->intra_process_mutex.lock();
    std::erase(connector->session->inprocs, connector);
    connector->session->intra_process_mutex.unlock();
    connector->identity.free();
    connector->~IntraProcessConnector(); // call destructor in-place
}
