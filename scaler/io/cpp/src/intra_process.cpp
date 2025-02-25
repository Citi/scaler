#include "intra_process.hpp"

void intraprocess_init(Session *session, IntraProcessClient *client, uint8_t *identity, size_t len)
{
    uint8_t *identity_dup = (uint8_t *)malloc(len * sizeof(uint8_t));
    std::memcpy(identity_dup, identity, len);

    new (client) IntraProcessClient{
        .session = session,
        .queue = ConcurrentQueue<Message>(),
        .recv = ConcurrentQueue<void *>(),
        .recv_buffer_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .recv_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .connecting = std::nullopt,
        .identity = Bytes{
            .data = identity_dup,
            .len = len,
        },
        .bind = std::nullopt,
        .peer = std::nullopt,
    };

    // take exclusive lock on the session to add the client
    session->intraprocess_mutex.lock();
    session->inprocs.push_back(client);
    session->intraprocess_mutex.unlock();
}

void intraprocess_bind(struct IntraProcessClient *client, const char *addr, size_t len)
{
    // use all params to avoid unused warnings
    (void)client;
    (void)addr;
    (void)len;

    panic("intraprocess_bind(): not implemented");
}

void intraprocess_connect(struct IntraProcessClient *client, const char *addr, size_t len)
{
    // use all params to avoid unused warnings
    (void)client;
    (void)addr;
    (void)len;

    panic("intraprocess_connect(): not implemented");
}

void intraprocess_send(struct IntraProcessClient *client, uint8_t *data, size_t len)
{
    // use all params to avoid unused warnings
    (void)client;
    (void)data;
    (void)len;

    panic("intraprocess_send(): not implemented");
}

void intraprocess_recv_sync(struct IntraProcessClient *client, struct Message *msg)
{
    // use all params to avoid unused warnings
    (void)client;
    (void)msg;

    panic("intraprocess_recv_sync(): not implemented");
}

void intraprocess_recv_async(void *future, struct IntraProcessClient *client)
{
    // use all params to avoid unused warnings
    (void)future;
    (void)client;

    panic("intraprocess_recv_async(): not implemented");
}
