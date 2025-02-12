// C
#include <cstddef>
#include <cstdint>
#include <cstring>

// C++
#include <thread>
#include <queue>
#include <vector>
#include <semaphore>
#include <optional>
#include <atomic>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <iostream>
#include <source_location>
#include <string>

// System
#include <sys/socket.h>
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/un.h>
#include <sys/timerfd.h>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>

#include <arpa/inet.h>
#include <netinet/tcp.h>

// Third-party
#include "third_party/concurrentqueue.h"
#include "third_party/blockingconcurrentqueue.h"

// we need to change certain settings depending on if we're compiling as C or C++
#ifdef COMPILE

// 5ca1ab1e = scalable
static uint8_t MAGIC[4] = {0x5c, 0xa1, 0xab, 0x1e};

#define ENUM enum class
#else
#define ENUM enum
#endif

using moodycamel::BlockingConcurrentQueue;
using moodycamel::ConcurrentQueue;

// max message size to receive or send in bytes
#define MAX_MSG_SIZE 500 * 1024 * 1024 // 500M

[[noreturn]] void panic(
    [[maybe_unused]] std::string message,
    const std::source_location &location = std::source_location::current())
{

    auto file_name = std::string(location.file_name());
    file_name = file_name.substr(file_name.find_last_of("/") + 1);

    std::cout << "panic at " << file_name << ":" << location.line()
              << ":" << location.column() << " in function ["
              << location.function_name() << "] in file ["
              << location.file_name() << "]: " << message << std::endl;

    exit(1);
}

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
            return false;

        return std::memcmp(data, other.data, len) == 0;
    }

    // same as empty()
    bool operator!() const
    {
        return len == 0 || data == nullptr;
    }

    bool empty() const
    {
        return len == 0 || data == nullptr;
    }

    // debugging utility
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
    // for received messages, the address data is
    // owned by the peer it was received from
    Bytes address;

    // the payload of the message
    //
    // for received messages, the payload data is owned
    // and must be freed when the message is destroyed
    Bytes payload;
};

struct Session;
struct IntraProcessClient;

ENUM EpollType{
    ClientSend,
    ClientRecv,
    ClientListener,
    ClientPeerRecv,
    IntraProcessClientRecv,
    PeerConnecting,

    ConnectTimer,
    Control,
    Closed,
};

// this is an in-progress write operation
// created only after the entire header has been written
struct WriteOperation
{
    SendMsg send;
    size_t cursor;
};

// an in-progress read operation
// created only after the entire header has been read
struct ReadOperation
{
    Message message;
    size_t cursor;
};

struct EpollData2
{
    int fd;
    EpollType type;
    std::optional<ReadOperation> read;
    std::optional<WriteOperation> write;

    union
    {
        void *ptr;
        Client *client;
        IntraProcessClient *inproc;
        Peer *peer;
    };
};

struct EpollData
{
    int fd;
    EpollType type;

    union
    {
        void *ptr;
        Client *client;
        IntraProcessClient *inproc;
        Peer *peer;
    };
};

// inproc sockets are always pair sockets
struct IntraProcessClient
{
    size_t id;
    Session *session;

    ConcurrentQueue<Message> queue;
    ConcurrentQueue<void *> recv;

    int recv_buffer_event_fd;
    int recv_event_fd;

    std::optional<std::string> connecting;

    Bytes identity;
    std::optional<std::string> bind;
    std::optional<size_t> peer;
};

ENUM ControlOperation{
    AddClient,
    RemoveClient,
    Connect,
};

struct ControlRequest
{
    ControlOperation op;
    int client_fd;
    std::optional<std::binary_semaphore> sem;
    std::optional<sockaddr_storage> addr;

    union
    {
        void *data;
        Client *client;
    };
};

struct ThreadContext
{
    Session *session;
    std::thread thread;
    std::vector<EpollData2 *> io_cache;
    std::vector<Peer *> connecting;
    ConcurrentQueue<ControlRequest> control;
    int control_efd;
    int epoll_fd;
    int connect_timer_tfd;
    bool timer_armed;

    void arm_timer()
    {
        const time_t timeout_s = 3;

        itimerspec timer{
            .it_interval = {.tv_sec = 0, .tv_nsec = 0},
            .it_value = {.tv_sec = timeout_s, .tv_nsec = 0},
        };

        if (timerfd_settime(this->connect_timer_tfd, 0, &timer, nullptr) < 0)
        {
            panic("failed to arm timer");
        }
        this->timer_armed = true;
    }

    void accept_peer(Peer *peer);

    void add_client(Client *client);
    void remove_client(Client *client);

    // must be called on io-thread
    void add_epoll(int fd, uint32_t flags, EpollType type, void *data)
    {
        auto edata = new EpollData2{
            .fd = fd,
            .type = type,
            .ptr = data,
        };

        epoll_event event{
            .events = flags,
            .data = {.ptr = edata}};

        epoll_ctl(this->epoll_fd, EPOLL_CTL_ADD, fd, &event);

        this->io_cache.push_back(edata);
    }

    // must be called on io-thread
    void remove_epoll(int fd)
    {
        if (epoll_ctl(this->epoll_fd, EPOLL_CTL_DEL, fd, nullptr) < 0)
        {
            // we ignore enoent because it means the fd was already removed
            if (errno != ENOENT)
                panic("failed to remove epoll fd: " + std::to_string(fd) + "; " + strerror(errno));
        }

        auto edata = std::find_if(this->io_cache.begin(), this->io_cache.end(), [fd](EpollData2 *d)
                                  { return d->fd == fd; });

        if (edata != this->io_cache.end())
        {
            delete *edata;
            this->io_cache.erase(edata);
        }
    }
};

struct Session
{
    // the io threads
    std::vector<ThreadContext> threads;
    std::vector<IntraProcessClient *> inprocs;

    std::shared_mutex intraprocess_mutex;

    int epoll_close_efd;

    std::atomic_uint8_t thread_rr;

    inline size_t num_threads()
    {
        return threads.size();
    };

    inline bool is_single_threaded()
    {
        return num_threads() == 1;
    };

    // void add_epoll_fd(int fd, EpollType type, void *data, bool edge_triggered = true);
    // bool epoll_data_by_fd(int fd, EpollData **data);
    // bool has_epoll_data_fd(int fd);
    // void remove_epoll_fd(int fd);
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
void io_thread_main(ThreadContext *ctx, size_t id);
ReadResult read_message(int fd, Bytes *data, bool stop_if_no_data, int timeout);
ReadResult readexact(int fd, uint8_t *data, size_t len, bool stop_if_no_data);
WriteResult write_message(int fd, Bytes *data);
WriteResult writeall(int fd, const uint8_t *data, size_t len);
void serialize_u32(uint32_t x, uint8_t buffer[4]);
void deserialize_u32(const uint8_t buffer[4], uint32_t *x);
bool client_connect_(Client *client, sockaddr_storage &&addr, int tries);
void set_sock_opts(int fd);

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

// Clients are tcp or unix domain sockets (uds, ipc)
// no variant for in-process because they're handled separately
ENUM Transport{
    TCP,
    IntraProcess,
    InterProcess};

ENUM SendResult{
    Sent,
    Muted};

struct Client
{
    ConnectorType type;
    Transport transport;

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

    int recv_event_fd;                    // event fd for recv queue
    ConcurrentQueue<void *> recv;         // the recv queue for io thread -> Python thread communication
    int recv_buffer_event_fd;             // event fd for recv buffer, only needed for sync clients
    ConcurrentQueue<Message> recv_buffer; // these are messages that have been received

    // must hold mutex
    bool peer_by_id(Bytes id, Peer **peer)
    {
        auto it = std::find_if(this->peers.begin(), this->peers.end(), [id](Peer *p)
                               { return p->identity == id; });

        if (it != this->peers.end())
        {
            *peer = *it;
            return true;
        }

        return false;
    }

    inline bool muted()
    {
        // these types mute when they have no peers
        if (this->type == ConnectorType::Pair || this->type == ConnectorType::Dealer)
        {
            return this->peers.empty();
        }

        // other types drop messages when they have no peers
        return false;
    }

    void recv_msg(Message &&msg);
    void unmute();
};

// public API
void client_init(Session *session, Client *binder, Transport transport, uint8_t *identity, size_t len, ConnectorType type);
void client_bind(Client *binder, const char *host, uint16_t port);
void client_connect(Client *binder, const char *addr, uint16_t port);
void client_send(void *future, Client *binder, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void client_recv(void *future, Client *binder);
void client_destroy(Client *binder);
void message_destroy(Message *recv);

void client_send_sync(Client *binder, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void client_recv_sync(Client *client, Message *msg);

void intraprocess_init(Session *session, IntraProcessClient *inproc, uint8_t *identity, size_t len);
void intraprocess_recv_async(void *future, IntraProcessClient *inproc);
void intraprocess_recv_sync(IntraProcessClient *inproc, Message *msg);
void intraprocess_send(IntraProcessClient *inproc, uint8_t *data, size_t len);
void intraprocess_connect(IntraProcessClient *inproc, const char *addr, size_t len);
void intraprocess_bind(IntraProcessClient *inproc, const char *addr, size_t len);

// Python callbacks
void future_set_result(void *future, void *data);

// private API
void peer_destroy(Peer *peer);
int fd_wait(int fd, int timeout, short int events);
uint8_t *datadup(const uint8_t *data, size_t len);

// todo: make this a method of Client
SendResult client_send_(Client *client, Message &&msg);

// epoll handlers
void client_send_event(Client *binder);
void client_recv_event(Client *binder);
void client_peer_recv_event(Peer *peer);
void client_listener_event(Client *binder);
void intraprocess_recv_event(IntraProcessClient *inproc);

void message_destroy(Message &message);

void ThreadContext::add_client(Client *client)
{
    this->add_epoll(client->send_event_fd, EPOLLIN | EPOLLET, EpollType::ClientSend, client);
    this->add_epoll(client->recv_event_fd, EPOLLIN | EPOLLET, EpollType::ClientRecv, client);
}

void ThreadContext::remove_client(Client *client)
{
    this->remove_epoll(client->send_event_fd);
    this->remove_epoll(client->recv_event_fd);
}
