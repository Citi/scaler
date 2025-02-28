#include "intra_process.hpp"

void IntraProcessClient::ensure_epoll() {
    if (this->epoll)
        return;

    this->epoll = true;

    this->thread->add_epoll(this->recv_buffer_event_fd, EPOLLIN | EPOLLET, EpollType::IntraProcessClientRecv, this);
    this->thread->add_epoll(this->recv_event_fd, EPOLLIN | EPOLLET, EpollType::IntraProcessClientRecv, this);
}

void IntraProcessClient::remove_from_epoll() {
    if (!this->epoll)
        return;

    this->thread->remove_epoll(this->recv_buffer_event_fd);
    this->thread->remove_epoll(this->recv_event_fd);
}

void intraprocess_init(Session *session, IntraProcessClient *client, uint8_t *identity, size_t len)
{
    new (client) IntraProcessClient{
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
    session->intraprocess_mutex.lock();
    session->inprocs.push_back(client);
    session->intraprocess_mutex.unlock();
}

void intraprocess_bind(IntraProcessClient *client, const char *addr, size_t len)
{
    if (client->bind)
        panic("intraprocess_bind(): client already bound");

    std::string bind(addr, len);

    client->session->intraprocess_mutex.lock();

    // check for conflicts
    for (size_t i = 0; i < client->session->inprocs.size(); i++)
    {
        auto other = client->session->inprocs[i];

        if (other == client)
            continue;

        if (other->bind == bind)
        {
            client->session->intraprocess_mutex.unlock();
            panic("intraprocess_bind(): address already in use");
        }
    }

    // set the bind address
    client->bind = bind;

    // check for any pending connections
    for (size_t i = 0; i < client->session->inprocs.size(); i++)
    {
        auto other = client->session->inprocs[i];

        if (other == client)
            continue;

        if (other->connecting == bind)
        {
            other->connecting = std::nullopt;
            other->peer = client;
            client->peer = other;

            if (eventfd_signal(other->unmuted_event_fd) < 0)
                panic("intraprocess_bind(): failed to signal unmuted_event_fd");
        }
    }
    client->session->intraprocess_mutex.unlock();
}

void intraprocess_connect(IntraProcessClient *client, const char *addr, size_t len)
{
    std::string connecting(addr, len);

    client->session->intraprocess_mutex.lock();

    for (size_t i = 0; i < client->session->inprocs.size(); i++)
    {
        auto other = client->session->inprocs[i];

        if (other == client)
            continue;

        // we found a matching bind
        if (client->bind == connecting)
        {
            other->peer = client;
            client->peer = other;

            if (eventfd_signal(other->unmuted_event_fd) < 0)
                panic("intraprocess_connect(): failed to signal unmuted_event_fd");

            client->session->intraprocess_mutex.unlock();
            return;
        }
    }

    // the connection is pending
    client->connecting = connecting;
    client->session->intraprocess_mutex.unlock();
}

void intraprocess_send(IntraProcessClient *client, uint8_t *data, size_t len)
{
    for (;;)
    {
        client->session->intraprocess_mutex.lock_shared();

        if (client->peer)
        {
            Message msg{
                .type = MessageType::Data,

                // we need to clone the identity because the sending client
                // has an independent lifetime from the message / receiving client
                .address = Bytes::clone(client->identity),

                // the caller (Python) owns the data, so we need to copy it
                .payload = Bytes::copy(data, len),
            };

            (*client->peer)->queue.enqueue(msg);

            // signal the receiving client (semaphore)
            if (eventfd_signal((*client->peer)->recv_buffer_event_fd) < 0)
                panic("intraprocess_send(): failed to signal recv_buffer_event_fd");

            client->session->intraprocess_mutex.unlock();
            return;
        }

        client->session->intraprocess_mutex.unlock_shared();

        // wait for a connection
        if (fd_wait(client->unmuted_event_fd, -1, POLLIN) < 0)
            panic("intraprocess_send(): failed to wait on unmuted_event_fd");

        if (eventfd_wait(client->unmuted_event_fd) < 0)
        {
            // pre-empted?
            if (errno == EAGAIN)
                continue;

            panic("intraprocess_send(): failed to wait on unmuted_event_fd");
        }
    }
}

void intraprocess_recv_sync(IntraProcessClient *client, Message *msg)
{
wait:
    if (fd_wait(client->recv_buffer_event_fd, -1, POLLIN) < 0)
        panic("intraprocess_recv_sync(): failed to wait on recv_buffer_event_fd");

    if (eventfd_wait(client->recv_buffer_event_fd) < 0)
    {
        if (errno == EAGAIN)
            goto wait; // pre-empted, try again

        panic("intraprocess_recv_sync(): failed to wait on recv_buffer_event_fd");
    }

    // after decrementing the semaphore, we have claimed the message from the queue
    // this also guarantees that the message is in the queue
    while (!client->queue.try_dequeue(*msg))
        ; // wait
}

void intraprocess_recv_async(void *future, IntraProcessClient *client)
{
    // ensure that the client is in the epoll
    // this allows sync-only clients to avoid epoll overhead
    client->ensure_epoll();
    client->recv.enqueue(future);

    if (eventfd_signal(client->recv_event_fd) < 0)
        panic("intraprocess_recv_async(): failed to signal recv_event_fd");
}
