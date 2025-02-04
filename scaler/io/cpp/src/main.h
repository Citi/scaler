#include <cstdint>
#include <cstddef>
#include <thread>
#include <queue>
#include <vector>
#include <semaphore>
#include <optional>
#include <atomic>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <sys/socket.h>

#include "third_party/concurrentqueue.h"
#include "third_party/blockingconcurrentqueue.h"

#ifdef ACTUALLY_CPP_PLEASE
#define ENUM enum class
#else
#define ENUM enum
#endif

using moodycamel::BlockingConcurrentQueue;
using moodycamel::ConcurrentQueue;

#define MAX_MSG_SIZE 10 * 1024 * 1024

struct Client;
struct Peer;
struct Message;
struct SendMsg;

struct Bytes
{
    uint8_t *data;
    size_t len;

    bool operator==(const Bytes &other) const
    {
        if (len != other.len)
        {
            return false;
        }

        for (size_t i = 0; i < len; i++)
        {
            if (data[i] != other.data[i])
            {
                return false;
            }
        }

        return true;
    }

    // is empty
    bool operator!() const
    {
        return len == 0 || data == nullptr;
    }

    bool empty() const
    {
        return len == 0 || data == nullptr;
    }

    std::string as_string() const
    {
        if (empty())
        {
            return "[EMPTY]";
        }

        return std::string((char *)data, len);
    }
};

struct Message
{
    // the address the message was received from, or to send to
    //
    // the contained data is not owned by the message
    // it's owned by the peer to which it belongs
    Bytes address;

    // the payload of the message
    //
    // the contained data is owned by the message
    // and must be freed when the message is destroyed
    Bytes payload;
};

struct ThreadCtx
{
    // the thread
    std::thread thread;

    // the thread's epoll fd
    // int epoll_fd;
};

struct Session;
struct Inproc;

ENUM EpollType{
    ClientSend,
    ClientRecv,
    ClientListener,
    ClientPeerRecv,
    InprocRecv,
    EpollClosed,
};

struct EpollData
{
    int fd;
    EpollType type;
    bool dead;

    union
    {
        void *ptr;
        Client *client;
        Inproc *inproc;
        Peer *peer;
    };
};

// inproc sockets are always pair sockets
struct Inproc
{
    size_t id;
    Session *session;

    // std::mutex mutex;

    ConcurrentQueue<Message> queue;
    ConcurrentQueue<void *> recv;

    int recv_buffer_event_fd;
    int recv_event_fd;

    std::optional<std::string> connecting;

    Bytes identity;
    std::optional<std::string> bind;
    std::optional<size_t> peer;
};

struct Session
{
    // the io threads
    std::vector<ThreadCtx> threads;
    std::vector<Client *> clients;

    std::vector<Inproc *> inprocs;

    // exclusive: odifying inprocs or clients (rare)
    // shared: sending / receiving messages (common)
    std::shared_mutex mutex;
    std::shared_mutex inproc_mutex;

    int epoll_fd;
    std::vector<EpollData> epoll_data;

    int epoll_close_efd;

    size_t id_counter;

    inline size_t num_threads()
    {
        return threads.size();
    };

    inline bool is_single_threaded()
    {
        return num_threads() == 1;
    };
};

ENUM ReadResult{
    Read,
    TimedOut,
    NoData,
    Disconnect};

ENUM WriteResult{
    Written,
    Disconnected};

// public API
void session_init(Session *session, size_t num_threads);
void session_destroy(Session *session);

// private API
void io_thread_main(Session *session, size_t id);
void add_epoll_fd(Session *session, int fd, EpollType type, void *data, bool edge_triggered = true);
void remove_epoll_fd(Session *session, int fd);
bool epoll_data_by_fd(Session *session, int fd, EpollData **data);
bool has_epoll_data_fd(Session *session, int fd);
ReadResult read_message(int fd, Bytes *data, bool stop_if_no_data, int timeout);
ReadResult readexact(int fd, uint8_t *data, size_t len, bool stop_if_no_data);
WriteResult write_message(int fd, Bytes *data);
WriteResult writeall(int fd, const uint8_t *data, size_t len);
void serialize_u32(uint32_t x, uint8_t buffer[4]);
void deserialize_u32(const uint8_t buffer[4], uint32_t *x);
bool client_connect_(Client *client, sockaddr_storage &&addr, int tries);
void set_sock_opts(int fd);

struct AddrInfo
{
    const char *host;
    uint16_t port;

    // empty
    bool operator!() const
    {
        return host == nullptr || port == 0;
    }

    bool empty() const
    {
        return host == nullptr || port == 0;
    }
};

struct Peer
{
    Client *client;        // the binder that this peer belongs to
    Bytes identity;        // the peer's address, i.e. identity
    sockaddr_storage addr; // the peer's address, if we are the connector
    int fd;                // the tcp socket fd of this peer
};

struct SendMsg
{
    // the future to resolve when the message is sent
    void *future;

    // the message to send
    Message msg;
};

ENUM ConnectorType{
    Pair,
    Pub,
    Sub,
    Dealer,
    Router // only valid for binder interface
};

ENUM ConnectOrBind{
    Uninit,
    Connect,
    Bind};

// Clients are tcp or unix domain sockets (uds, ipc)
// no variant for inproc because they're handled differently
ENUM ClientTransport{
    Tcp,
    Uds};

ENUM SendResult{
    Sent,
    Muted};

struct Client
{
    size_t id;

    ConnectorType type;
    ClientTransport transport;

    // this mutex protects everything except thread-safe things
    // because we don't want multiple io threads modifying the same data
    // such as: concurrent queues, and event fds
    std::mutex mutex;

    Session *session; // backreference to session
    Bytes identity;   // the identity of this client

    int rr; // round robin for dealer

    int fd;                               // the bound socket, <0 when not bound
    std::optional<sockaddr_storage> addr; // addr for when we're bound
    std::vector<Peer *> peers;

    int unmuted_event_fd; // event fd for when the client is no longer muted

    int send_event_fd;             // event fd for send queue
    ConcurrentQueue<SendMsg> send; // the send queue for Python thread -> io thread communication
    std::queue<SendMsg> muted;     // messages that are muted because the socket is not connected

    int recv_event_fd;                    // event fd for recv queue
    ConcurrentQueue<void *> recv;         // the recv queue for io thread -> Python thread communication
    int recv_buffer_event_fd;             // event fd for recv buffer, only needed for sync clients
    ConcurrentQueue<Message> recv_buffer; // these are messages that have been received

    // must hold mutex
    bool peer_by_id(Bytes id, Peer **peer);
    void recv_msg(Message &&msg);
};

// public API
void client_init(Session *session, Client *binder, ClientTransport transport, uint8_t *identity, size_t len, ConnectorType type);
void client_bind(Client *binder, const char *host, uint16_t port);
void client_connect(Client *binder, const char *addr, uint16_t port);
void client_send(void *future, Client *binder, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void client_recv(void *future, Client *binder);
void client_destroy(Client *binder);
void message_destroy(Message *recv);

void client_send_sync(Client *binder, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void client_recv_sync(Client *client, Message *msg);

void inproc_init(Session *session, Inproc *inproc, uint8_t *identity, size_t len);
void inproc_recv_async(void *future, Inproc *inproc);
void inproc_recv_sync(Inproc *inproc, Message *msg);
void inproc_send(Inproc *inproc, uint8_t *data, size_t len);
void inproc_connect(Inproc *inproc, const char *addr, size_t len);
void inproc_bind(Inproc *inproc, const char *addr, size_t len);

// Python callbacks
/*extern "Python+C"*/ void future_set_result(void *future, void *data);

// private API
// void client_connect_tcp_(Client *binder, const char *addr, uint16_t port, bool force);
// void client_reconnect(Client *binder);
void peer_destroy(Peer *peer);
int fd_wait(int fd, int timeout, short int events);

// epoll handlers
void client_send_event(Client *binder);
void client_recv_event(Client *binder);
void client_peer_recv_event(Peer *peer);
void client_listener_event(Client *binder);
void inproc_recv_event(Inproc *inproc);

#ifdef REAL

// 5ca1ab1e = scalable
static uint8_t MAGIC[4] = {0x5c, 0xa1, 0xab, 0x1e};
#endif
