// this file contains C-compatible definitions for the C++ code in the other files
// this is the interface exposed to Python

struct Bytes
{
    uint8_t *data;
    size_t len;
};

enum MessageType {
    Data,
    Identity,
    Disconnect,
};

struct Message
{
    enum MessageType type;
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
    // this means that Python doesn't know about the internals of the struct
    // the compiler will figure out the size of the struct for us
    // based upon the full definition in the C++ code
    ...;
};

struct NetworkConnector
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

void connector_init(struct Session *session, struct NetworkConnector *connector, enum Transport transport, uint8_t *identity, size_t len, enum ConnectorType type);
void connector_bind(struct NetworkConnector *connector, const char *host, uint16_t port);
void connector_connect(struct NetworkConnector *connector, const char *addr, uint16_t port);
void connector_send(void *future, struct NetworkConnector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void connector_send_sync(struct NetworkConnector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void connector_recv(void *future, struct NetworkConnector *connector);
void connector_recv_sync(struct NetworkConnector *connector, struct Message *msg);
void connector_destroy(struct NetworkConnector *connector);

void intraprocess_init(struct Session *session, struct IntraProcessClient *client, uint8_t *identity, size_t len);
void intraprocess_bind(struct IntraProcessClient *client, const char *addr, size_t len);
void intraprocess_connect(struct IntraProcessClient *client, const char *addr, size_t len);
void intraprocess_send(struct IntraProcessClient *client, uint8_t *data, size_t len);
void intraprocess_recv_sync(struct IntraProcessClient *client, struct Message *msg);
void intraprocess_recv_async(void *future, struct IntraProcessClient *client);
void intraprocess_destroy(struct IntraProcessClient *client);

void message_destroy(struct Message *message);
