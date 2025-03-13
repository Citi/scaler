#pragma once

// C++
#include <atomic>
#include <shared_mutex>
#include <vector>
#include <queue>
#include <thread>

// System
#include <sys/epoll.h>
#include <sys/timerfd.h>

// Common
#include "common.hpp"

// --- declarations ---

struct EpollType;
struct EpollData;
ENUM ControlOperation : uint8_t;
struct ControlRequest;
struct ThreadContext;
struct Session;

// First-party
#include "client.hpp"
#include "intra_process.hpp"

void set_sock_opts(int fd);
void complete_peer_connect(RawPeer *peer);
void connector_connect_peer(RawPeer *peer);

bool read_identity(RawPeer *peer);
bool write_identity(RawPeer *peer);

// epoll handlers
void connector_send_event(NetworkConnector *connector);
void connector_recv_event(NetworkConnector *connector);
void connector_listener_event(NetworkConnector *connector);
void connector_destroy_timeout(NetworkConnector *connector);
void connector_peer_event_connecting(epoll_event *event);
void connector_peer_event_connected(epoll_event *event);
void intraprocess_recv_event(IntraProcessClient *client);

void io_thread_main(ThreadContext *ctx);

void session_init(Session *session, size_t num_threads);
void session_destroy(Session *session);

// --- structs ---

struct EpollType
{
    enum Value
    {
        ClientSend,
        ClientRecv,
        ClientListener,
        ClientPeer,
        IntraProcessClientRecv,

        ConnectTimer,
        Control,
        Closed,
        ClientDestroyTimeout,
    };

    constexpr EpollType(Value value) : value(value) {}
    constexpr operator Value() const { return value; }

    std::string as_string() const;

private:
    Value value;
};

struct EpollData
{
    int fd;
    EpollType type;

    union
    {
        void *ptr;
        NetworkConnector *connector;
        IntraProcessClient *inproc;
        RawPeer *peer;
    };
};

ENUM ControlOperation : uint8_t{
                            AddClient,
                            DestroyClient,
                            Connect,
                        };

struct ControlRequest
{
    ControlOperation op;
    Completer completer;

    union
    {
        void *data;
        NetworkConnector *connector;
        RawPeer *peer;
    };

    void complete(void *result = NULL)
    {
        completer.complete(result);
    }
};

struct ThreadContext
{
    size_t id;
    Session *session;
    std::thread thread;
    std::vector<EpollData *> io_cache;
    std::vector<RawPeer *> connecting;
    ConcurrentQueue<ControlRequest> control_queue;
    int control_efd;
    int epoll_fd;
    int epoll_close_efd;
    int connect_timer_tfd;
    bool timer_armed;

    void ensure_timer_armed();
    void add_client(NetworkConnector *connector);
    void add_peer(RawPeer *peer);
    void remove_client(NetworkConnector *connector);
    void remove_peer(RawPeer *peer);

    // must be called on io-thread
    void add_epoll(int fd, uint32_t flags, EpollType type, void *data);

    // must be called on io-thread
    void remove_epoll(int fd);

    EpollData *epoll_by_fd(int fd);

    void control(ControlRequest request);

    void start();
};

struct Session
{
    // the io threads
    std::vector<ThreadContext> threads;
    std::vector<IntraProcessClient *> inprocs;

    std::shared_mutex intraprocess_mutex;

    std::atomic_uint8_t thread_rr;

    inline size_t num_threads()
    {
        return threads.size();
    };

    ThreadContext *next_thread()
    {
        auto rr = thread_rr++;
        return &threads[rr % num_threads()];
    }

    inline bool is_single_threaded()
    {
        return num_threads() == 1;
    };
};
