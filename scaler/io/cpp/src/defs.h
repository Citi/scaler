struct Bytes
{
    uint8_t *data;
    size_t len;
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

enum Transport
{
    TCP,
    IntraProcess,
    InterProcess
};

struct Session
{
    ...;
};

struct Client
{
    ...;
};

struct IntraProcessClient
{
    ...;
};

// Python callback
extern "Python+C" void future_set_result(void *future, void *data);

void session_init(struct Session *session, size_t num_threads);
void session_destroy(struct Session *session);

void client_init(struct Session *session, struct Client *client, enum Transport transport, uint8_t *identity, size_t len, enum ConnectorType type);
void client_bind(struct Client *client, const char *host, uint16_t port);
void client_connect(struct Client *client, const char *addr, uint16_t port);
void client_send(void *future, struct Client *client, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void client_send_sync(struct Client *client, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void client_recv(void *future, struct Client *client);
void client_recv_sync(struct Client *client, struct Message *msg);
void client_destroy(struct Client *client);

void intraprocess_init(struct Session *session, struct IntraProcessClient *client, uint8_t *identity, size_t len);
void intraprocess_bind(struct IntraProcessClient *client, const char *addr, size_t len);
void intraprocess_connect(struct IntraProcessClient *client, const char *addr, size_t len);
void intraprocess_send(struct IntraProcessClient *client, uint8_t *data, size_t len);
void intraprocess_recv_sync(struct IntraProcessClient *client, struct Message *msg);
void intraprocess_recv_async(void *future, struct IntraProcessClient *client);
