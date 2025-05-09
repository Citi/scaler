// this file contains C-compatible definitions for the C++ code in the other files
// this is the interface exposed to Python

struct Bytes
{
    uint8_t *data;
    size_t len;
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
    Router
};

enum Transport
{
    TCP,
    IntraProcess,
    InterProcess
};

struct IoContext
{
    // this means that Python doesn't know about the internals of the struct
    // the compiler will figure out the size of the struct for us
    // based upon the full definition in the C++ code
    ...;
};

struct Connector
{
    ...;
};

enum Code
{
    AlreadyBound,
    InvalidAddress,
    UnsupportedOperation,
    NoThreads,
    PeerShutdown
};

enum StatusType
{
    Ok,
    Logical,
    Errno,
    Signal
};

struct Status
{
    enum StatusType type;
    const char *message;
    union
    {
        enum Code code;
        int no;
        int signal;
    };
};

// Python callback
extern "Python+C" void future_set_result(void *future, void *data);
extern "Python+C" void future_set_status(void *future, void *status);

struct Status io_context_init(struct IoContext *ioctx, size_t num_threads);
struct Status io_context_destroy(struct IoContext *ioctx, bool destruct);
void message_destroy(struct Message *message);

struct Status connector_init(struct IoContext *ioctx, struct Connector *connector, enum Transport transport, enum ConnectorType type, uint8_t *identity, size_t len);
struct Status connector_destroy(struct Connector *connector);
struct Status connector_bind(struct Connector *connector, const char *host, uint16_t port);
struct Status connector_connect(struct Connector *connector, const char *host, uint16_t port);
void connector_send_async(void *future, struct Connector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
struct Status connector_send_sync(struct Connector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void connector_recv_async(void *future, struct Connector *connector);
struct Status connector_recv_sync(struct Connector *connector, struct Message *msg);
