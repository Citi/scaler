// COMMON

struct Bytes
{
    uint8_t *data;
    size_t len;
};

struct Session
{
    ...;
};

struct Client
{
    ...;
};

struct Inproc {
    ...;
};

struct Message
{
    struct Bytes address;
    struct Bytes payload;
};

enum ConnectorType
{
    Pair,
    Sub,
    Pub,
    Dealer,
    Router // only valid for binder interface
};

enum ClientTransport {
    Tcp,
    Uds
};

void session_init(struct Session *session, size_t num_threads);
void session_destroy(struct Session *session);
void client_init(struct Session *session, struct Client *binder, enum ClientTransport transport, uint8_t *identity, size_t len, enum ConnectorType type);
void client_bind(struct Client *binder, const char *host, uint16_t port);
void client_connect(struct Client *binder, const char *addr, uint16_t port);
void client_send(void *future, struct Client *binder, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void client_recv(void *future, struct Client *binder);
void client_destroy(struct Client *binder);
void message_destroy(struct Message *recv);

void client_send_sync(struct Client *binder, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void client_recv_sync(struct Client *client, struct Message *msg);

extern "Python+C" void future_set_result(void *future, void *data);

void inproc_init(struct Session *session, struct Inproc *inproc, uint8_t *identity, size_t len);
void inproc_recv_async(void *future, struct Inproc *inproc);
void inproc_recv_sync(struct Inproc *inproc, struct Message *msg);
void inproc_send(struct Inproc *inproc, uint8_t *data, size_t len);
void inproc_connect(struct Inproc *inproc, const char *addr, size_t len);
void inproc_bind(struct Inproc *inproc, const char *addr, size_t len);
