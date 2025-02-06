
#define COMPILE 1

#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <semaphore>
#include <iostream>
#include <source_location>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/signalfd.h>
#include <poll.h>
#include <signal.h>
#include <sys/un.h>

#include "main.h"

#define DEBUG

/* #ifdef DEBUG
 #define dprint(fmt, ...)              \
     do                                \
     {                                 \
         std::println(fmt, __VA_ARGS__); \
     } while (0)
 #else
#define dprint(fmt, ...)
#endif */

#define dprint(fmt, ...)

extern "C"
{
    __attribute__((no_instrument_function)) void __cyg_profile_func_enter(void *, void *);

    void __cyg_profile_func_enter([[maybe_unused]] void *this_fn, [[maybe_unused]] void *call_site)
    {
        // std::cout << "entering function" << std::endl;
    }

    __attribute__((no_instrument_function)) void __cyg_profile_func_exit(void *, void *);

    void __cyg_profile_func_exit([[maybe_unused]] void *this_fn, [[maybe_unused]] void *call_site)
    {
        // std::cout << "exiting function" << std::endl;
    }
}

// COMMON

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

    // std::print(std::cerr, "panic at {}:{}:{} in function [{}] in file [{}]: {}\n",
    //            file_name, location.line(), location.column(),
    //            location.function_name(), location.file_name(), message);

    exit(1);
}

int easy_hash(const uint8_t *data, size_t len)
{
    int hash = 0;
    for (size_t i = 0; i < len; i++)
    {
        hash = (hash << 5) - hash + data[i];
    }

    return hash;
}

std::string event_name(EpollType &type)
{
    switch (type)
    {
    case EpollType::ClientSend:
        return "ClientSend";
    case EpollType::ClientRecv:
        return "ClientRecv";
    case EpollType::ClientPeerRecv:
        return "ClientPeerRecv";
    case EpollType::ClientListener:
        return "ClientListener";
    case EpollType::IntraProcessClientRecv:
        return "IntraProcessClientRecv";
    case EpollType::EpollClosed:
        return "EpollClosed";
    }

    panic("unknown epoll type");
}

void serialize_u32(uint32_t x, uint8_t buffer[4])
{
    buffer[0] = x & 0xFF;
    buffer[1] = (x >> 8) & 0xFF;
    buffer[2] = (x >> 16) & 0xFF;
    buffer[3] = (x >> 24) & 0xFF;
}

void deserialize_u32(const uint8_t buffer[4], uint32_t *x)
{
    *x = buffer[0] | buffer[1] << 8 | buffer[2] << 16 | buffer[3] << 24;
}

WriteResult writeall(int fd, const uint8_t *data, size_t len)
{
    size_t sent = 0;
    while (sent < len)
    {
        ssize_t n = write(fd, data + sent, len - sent);
        if (n < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                // std::cout << "writeall(): stalled" << std::endl;

                if (auto sig = fd_wait(fd, -1, POLLOUT))
                    panic("failed to wait on fd; signal: " + std::to_string(sig));

                continue;
            }

            if (errno == ECONNRESET)
            {
                return WriteResult::Disconnected;
            }

            panic("failed to send to peer: " + std::to_string(errno) + "; " + strerror(errno));
        }

        sent += n;
    }

    return WriteResult::Written;
}

char *to_hex(uint8_t *data, size_t len)
{
    char *hex = (char *)malloc((len * 3 + 1) * sizeof(char));
    for (size_t i = 0; i < len; i++)
    {
        sprintf(hex + i * 3, "%02x ", data[i]);
    }

    hex[len * 3] = '\0';
    return hex;
}

ReadResult readexact(int fd, uint8_t *data, size_t len, bool stop_if_no_data, int timeout)
{
    size_t n_read = 0;
    while (n_read < len)
    {
        ssize_t n = read(fd, data + n_read, len - n_read);
        if (n < 0)
        {
            if (errno == EWOULDBLOCK || errno == EAGAIN)
            {
                // if the socket had no data and we can stop early, return
                if (stop_if_no_data && n_read == 0)
                    return ReadResult::NoData;

                // otherwise, wait on fd to be readable
                // this could be made more efficient by saving our state and going back to epoll
                if (auto sig = fd_wait(fd, timeout, POLLIN))
                    panic("failed to wait on fd; signal: " + std::to_string(sig));

                continue;
            }

            if (errno == ECONNRESET)
            {
                return ReadResult::Disconnect;
            }

            panic("failed to read from peer: " + std::to_string(errno) + "; " + strerror(errno) + " ;; fd " + std::to_string(fd));
        }

        if (n == 0)
            return ReadResult::Disconnect;

        n_read += n;
    }

    return ReadResult::Read;
}

WriteResult write_message(int fd, Bytes *bytes)
{
    if (bytes->len > MAX_MSG_SIZE)
        panic("cannot write message; too large: " + std::to_string(bytes->len) + " bytes");

    uint8_t header[4];
    serialize_u32(htonl((uint32_t)bytes->len), header);

    auto status = writeall(fd, MAGIC, 4);
    if (status != WriteResult::Written)
        return status;

    status = writeall(fd, header, 4);
    if (status != WriteResult::Written)
        return status;

    return writeall(fd, bytes->data, bytes->len);
}

ReadResult read_message(int fd, Bytes *data, bool stop_if_no_data, int timeout)
{
    uint8_t magic[4];
    ReadResult status = readexact(fd, magic, 4, stop_if_no_data, timeout);

    // bubble-up the error
    if (status != ReadResult::Read)
        return status;

    // todo: handle this gracefully
    // it probably means we need to reconnect
    if (memcmp(magic, MAGIC, 4) != 0)
    {
        panic("invalid start magic: " + std::string(to_hex(magic, 4)));
    }

    uint8_t header[4];
    status = readexact(fd, header, 4, false, timeout);

    if (status != ReadResult::Read)
        return status;

    uint32_t len;
    deserialize_u32(header, &len);
    len = ntohl(len);

    uint8_t *buffer = (uint8_t *)malloc(len * sizeof(uint8_t));
    status = readexact(fd, buffer, len, false, timeout);

    if (status != ReadResult::Read)
    {
        free(buffer);
        return status;
    }

    data->data = buffer;
    data->len = len;

    return ReadResult::Read;
}

bool epoll_data_by_fd(Session *session, int fd, EpollData **data)
{
    auto x = std::find_if(session->epoll_data.begin(), session->epoll_data.end(), [fd](EpollData &d)
                          { return d.fd == fd; });

    if (x != session->epoll_data.end())
    {
        *data = &*x;
        return true;
    }

    return false;
}

void io_thread_main(Session *session, [[maybe_unused]] size_t id)
{
    for (;;)
    {
        std::cout << "io-thread[" << id << "]: waiting for events" << std::endl;

        epoll_event event;
        auto n_events = epoll_wait(session->epoll_fd, &event, 1, -1);

        std::cout << "io-thread[" << id << "]: got " << n_events << " events" << std::endl;

        if (n_events == 0)
        {
            continue;
        }

        if (n_events < 0)
        {
            // we were interrupted by a signal, wait again
            if (errno == EINTR)
                continue;

            // if the epoll fd is closed, we should exit
            if (errno == EBADF)
                return;

            panic("handle epoll error: " + std::to_string(errno) + "; " + strerror(errno));
        }

        // Q: why does the session need its own mutex?
        // A:
        //  - the session is shared between all threads, INCLUDING the Python thread
        //  - the Python thread can add or remove clients and inprocs
        //    **this can happen in the time between epoll_wait() returns and the event is processed
        std::cout << "io-thread[" << id << "]: locking session: " << event.data.fd << std::endl;
        while (!session->mutex.try_lock_shared())
        {
            std::cout << "io-thread[" << id << "]: waiting for session lock" << std::endl;

            // sleep
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        std::cout << "io-thread[" << id << "]: session locked: " << event.data.fd << std::endl;

        // note, the epoll data will only be valid while the shared lock is held
        EpollData *data;
        if (!epoll_data_by_fd(session, event.data.fd, &data))
        {
            std::cout << "io-thread[" << id << "]: failed to find epoll data for fd: " << event.data.fd << std::endl;
            session->mutex.unlock_shared();
            continue;
        }

        std::cout << "io-thread[" << id << "]: processing event: " << event_name(data->type) << std::endl;

        // clang-format off
        switch (data->type)
        {
            // the client has issued a send() call
            case EpollType::ClientSend:             client_send_event(data->client);            break;

            // the client has issued a recv() call
            case EpollType::ClientRecv:             client_recv_event(data->client);            break;

            // we have received a message from a peer
            case EpollType::ClientPeerRecv:         client_peer_recv_event(data->peer);         break;

            // we are bound and have a connection to accept
            case EpollType::ClientListener:         client_listener_event(data->client);        break;

            // an inproc client has received a message
            case EpollType::IntraProcessClientRecv: intraprocess_recv_event(data->inproc);      break;

            // the session is closing
            case EpollType::EpollClosed: {
                session->mutex.unlock_shared();
                return;
            }
        }
        // clang-format on

        std::cout << "io-thread[" << id << "]: unlocking session" << std::endl;
        // session->mutex.unlock_shared();
    }
}

void session_init(Session *session, size_t num_threads)
{
    new (session) Session{
        .threads = std::vector<std::thread>(),
        .clients = std::vector<Client *>(),
        .inprocs = std::vector<IntraProcessClient *>(),
        .mutex = std::shared_mutex(),
        .intraprocess_mutex = std::shared_mutex(),
        .epoll_fd = epoll_create1(0),
        .epoll_data = std::vector<EpollData>(),
        .epoll_close_efd = eventfd(0, 0),
        .id_counter = 0,
    };

    // this epfd is used to signal the io threads to close
    // unlike all others, it's level-triggered
    add_epoll_fd(session, session->epoll_close_efd, EpollType::EpollClosed, NULL, false);

    session->threads.reserve(num_threads);

    for (size_t i = 0; i < num_threads; ++i)
    {
        session->threads.emplace_back(
            std::thread(io_thread_main, session, i));
    }
}

void session_destroy(Session *session)
{
    std::cout << "destroying session" << std::endl;

    // this will wake up the io threads and cause them to exit
    if (eventfd_write(session->epoll_close_efd, 1) < 0)
    {
        panic("failed to write to epoll close eventfd");
    }

    // wait for all threads to exit
    for (size_t i = 0; i < session->threads.size(); ++i)
    {
        std::cout << "joining thread " << i << std::endl;
        session->threads[i].join();
    }

    std::cout << "all threads exited" << std::endl;

    close(session->epoll_fd);

    // call the destructor without freeing the memory (it's owned by the caller)
    session->~Session();
}

bool has_epoll_data_fd(Session *session, int fd)
{
    for (auto &d : session->epoll_data)
    {
        if (d.fd == fd)
        {
            return true;
        }
    }

    return false;
}

void add_epoll_fd(Session *session, int fd, EpollType type, void *data, bool edge_triggered)
{
    std::cout << "adding epoll fd: " << fd << " ;; " << event_name(type) << std::endl;

    EpollData epoll_data{
        .fd = fd,
        .type = type,
        .ptr = data,
    };

    if (has_epoll_data_fd(session, fd))
    {
        panic("epoll fd already exists: " + std::to_string(fd));
    }

    session->epoll_data.push_back(epoll_data);

    uint32_t flags = EPOLLIN;

    if (edge_triggered)
    {
        flags |= EPOLLET;
    }

    epoll_event event{
        // epollin: read events
        // epollet: edge-triggered
        // epollexclusive: only wake one* epoll instance
        // *optimistic, it can still wake multiple
        .events = flags,
        .data = {.fd = fd}};

    if (epoll_ctl(session->epoll_fd, EPOLL_CTL_ADD, fd, &event) < 0)
    {
        panic("failed to add epoll fd: " + std::to_string(fd) + "; " + strerror(errno));
    }
}

// must hold exclusive lock on session
void remove_epoll_fd(Session *session, int fd)
{
    if (epoll_ctl(session->epoll_fd, EPOLL_CTL_DEL, fd, nullptr) < 0)
    {
        // we ignore enoent because it means the fd was already removed
        if (errno != ENOENT)
            panic("failed to remove epoll fd: " + std::to_string(fd) + "; " + strerror(errno));
    }

    std::erase_if(session->epoll_data, [fd](EpollData &data)
                  { return data.fd == fd; });
}

bool Client::peer_by_id(Bytes id, Peer **peer)
{
    for (auto &p : this->peers)
    {
        if (p->identity == id)
        {
            *peer = p;
            return true;
        }
    }

    return false;
}

void client_init(Session *session, Client *client, Transport transport, uint8_t *identity, size_t len, ConnectorType type)
{
    // todo: error handling?
    if (transport == Transport::IntraProcess)
    {
        panic("IntraProcess transport has a separate API");
    }

    // *identity is owned by the caller, we need our own copy
    uint8_t *identity_dup = (uint8_t *)malloc(len * sizeof(uint8_t));
    memcpy(identity_dup, identity, len);

    auto id = session->id_counter++;

    new (client) Client{
        .id = id,
        .type = type,
        .transport = transport,

        .mutex = std::mutex(),
        .session = session,

        .identity = Bytes{
            .data = identity_dup,
            .len = len,
        },

        .rr = 0,

        .fd = -1,
        .addr = std::nullopt,
        .peers = std::vector<Peer *>(),

        .unmuted_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),

        .send_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .send = ConcurrentQueue<SendMsg>(),
        .muted = std::queue<SendMsg>(),

        .recv_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .recv = ConcurrentQueue<void *>(),
        // .outstanding_recvs = std::queue<void *>(),
        .recv_buffer_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .recv_buffer = ConcurrentQueue<Message>(),
    };

    // std::cout << "!! new client, identity: " << client->identity.as_string() << std::endl;

    std::cout << "client_init(): lock session" << std::endl;
    session->mutex.lock();
    session->clients.push_back(client);
    add_epoll_fd(session, client->send_event_fd, EpollType::ClientSend, client);
    add_epoll_fd(session, client->recv_event_fd, EpollType::ClientRecv, client);
    session->mutex.unlock();
}

void client_bind(Client *client, const char *host, uint16_t port)
{
    // std::cout << "binding to " << host << ":" << port << std::endl;

    auto session = client->session;
    client->mutex.lock();

    auto fd = socket(
        // note: this will break if additional transports are added
        // client->transport == ClientTransport::Tcp ? AF_INET : AF_UNIX,
        AF_INET,
        SOCK_STREAM | SOCK_NONBLOCK,
        0);

    int status = -1;
    sockaddr_storage addr;

    // switch (client->transport)
    // {
    // case ClientTransport::Tcp:
    {
        // std::cout << "TCP TRANSPORT" << std::endl;

        // int on = 1;
        // setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

        set_sock_opts(fd);

        in_addr_t in_addr = strncmp(host, "*", 1) ? inet_addr(host) : INADDR_ANY;

        sockaddr_in server_addr{
            .sin_family = AF_INET,
            .sin_port = htons(port),
            .sin_addr = {
                .s_addr = in_addr},
            .sin_zero = {0},
        };

        memcpy(&addr, &server_addr, sizeof(server_addr));

        status = bind(fd, (sockaddr *)&server_addr, sizeof(server_addr));
    }
    // break;
    // case ClientTransport::Uds:
    // {
    //     sockaddr_un server_addr{
    //         .sun_family = AF_UNIX,
    //         .sun_path = {0},
    //     };

    //     // copy the host into the sun_path
    //     strncpy(server_addr.sun_path, host, sizeof(server_addr.sun_path) - 1);

    //     status = bind(fd, (sockaddr *)&server_addr, sizeof(server_addr));
    // }
    // }

    if (status < 0)
    {
        if (errno == EADDRINUSE)
        {
            panic("address in use: " + std::string(host) + ":" + std::to_string(port));
        }

        panic("failed to bind socket");
    }

    if (listen(fd, 128) < 0)
    {
        panic("failed to listen on socket");
    }

    client->fd = fd;
    client->addr = addr;

    // locking the session after the client could cause a deadlock
    // if an io-thread holding the session lock tries to lock the client
    // we make an assumption that the client is not in the session list until it has been bound or connect
    // AND it will only ever be bound or connected, never both
    std::cout << "client_bind(): lock session" << std::endl;
    session->mutex.lock();
    add_epoll_fd(session, client->fd, EpollType::ClientListener, client);
    session->mutex.unlock();
    client->mutex.unlock();
}

// preconditions:
// - no locks held
//
// postconditions:
// - client unlocked
// - session unlocked
//
// return value:
// - true: client connected
// - false: client not connected
bool client_connect_(Client *client, sockaddr_storage &&addr, int tries)
{
    auto session = client->session;
    int attempt = 0;
start:
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0)
    {
        panic("failed to create socket");
    }

    set_sock_opts(fd);

    if (connect(fd, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        auto error = errno;
        socklen_t len = sizeof(error);

        if (errno == EINPROGRESS)
        {
            if (auto sig = fd_wait(fd, 2000, POLLOUT))
            {
                if (sig == SIGINT || sig == SIGQUIT || sig == SIGTERM)
                {
                    panic("recv signal during client connect: " + std::to_string(sig));
                }

                panic("failed to wait for connect");
            }

            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0)
            {
                panic("failed to check socket error");
            }
        }

        if (error == ECONNREFUSED || error == ETIMEDOUT)
        {
            close(fd);

            std::this_thread::sleep_for(std::chrono::seconds(1));
            if (tries > 0 && ++attempt >= tries)
            {
                return false;
            }

            goto start;
        }
        else if (error)
        {
            panic("failed to connect to socket; errno: " + std::to_string(error) + "; " + strerror(error));
        }
    }

    // std::cout << "client_connect_(): connected, writing identity to fd: " << fd << std::endl;
    // // std::cout << "connected to:" << std::string(host) << ":" << std::to_string(port) << ", writing identity: " << std::string((char *)client->identity.data, client->identity.len) << std::endl;

    if (write_message(fd, &client->identity) == WriteResult::Disconnected)
    {
        panic("peer disconnected during identity exchange");
    }

    Bytes identity;
    auto status = read_message(fd, &identity, false, 3000);
    if (status == ReadResult::Disconnect || status == ReadResult::TimedOut)
    {
        panic("peer disconnected during identity exchange");
    }

    // std::cout << "identity received: " << identity.len << std::endl;

    if (fd == 0)
    {
        panic("client connect: fd is 0???");
    }

    auto peer = new Peer{
        .client = client,
        .identity = identity,
        .addr = addr,
        .fd = fd,
    };

    client->mutex.lock();
    client->peers.push_back(peer);
    client->mutex.unlock();

    std::cout << "client_connect_(): lock session" << std::endl;
    session->mutex.lock();
    std::cout << "add client peer recv: " << fd << " for client: " << client->identity.as_string() << std::endl;
    add_epoll_fd(session, peer->fd, EpollType::ClientPeerRecv, peer);
    session->mutex.unlock();
    return true;
}

void client_connect(Client *client, const char *host, uint16_t port)
{
    // std::cout << "client_connect(): connecting to " << host << ":" << port << std::endl;

    sockaddr_in server_addr{
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {.s_addr = inet_addr(host)},
        .sin_zero = {0},
    };

    sockaddr_storage addr;
    memcpy(&addr, &server_addr, sizeof(server_addr));

    client_connect_(client, std::move(addr), -1);
}

uint8_t *datadup(const uint8_t *data, size_t len)
{
    uint8_t *dup = (uint8_t *)malloc(len * sizeof(uint8_t));
    memcpy(dup, data, len);
    return dup;
}

void client_send(void *future, Client *client, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len)
{
    // note: *to and *data buffers should be valid until *future is resolved
    auto send = SendMsg{
        .future = future,
        .msg = Message{
            .address = Bytes{to, to_len},
            .payload = Bytes{data, data_len},
        }};

    std::cout << "client_send(): sending message to: " << send.msg.address.as_string() << std::endl;
    client->send.enqueue(send);

    std::cout << "client_send(): enqueued message" << std::endl;

    // notify io threads
    if (eventfd_write(client->send_event_fd, 1) < 0)
    {
        panic("failed to write to eventfd: " + std::to_string(errno));
    }
}

void client_recv(void *future, Client *client)
{
    // std::cout << "client_recv(): waiting for message" << std::endl;

    client->recv.enqueue(future);

    // notify io threads
    if (eventfd_write(client->recv_event_fd, 1) < 0)
    {
        panic("failed to write to eventfd: " + std::to_string(errno));
    }
}

// wait on a single file descriptor
// this should work even on non-blocking fds
int fd_wait(int fd, int timeout, short int events)
{
    pollfd fds[2];

    fds[0] = {
        .fd = fd,
        .events = events,
        .revents = 0,
    };

    sigset_t sigs;
    sigemptyset(&sigs);
    sigaddset(&sigs, SIGINT);
    sigaddset(&sigs, SIGQUIT);
    sigaddset(&sigs, SIGTERM);

    auto signal_fd = signalfd(-1, &sigs, 0);

    fds[1] = {
        .fd = signal_fd,
        .events = POLLIN,
        .revents = 0,
    };

    auto n = poll(fds, 2, timeout);

    if (n == 0)
    {
        close(signal_fd);
        panic("poll timed out");
    }

    if (n < 0)
    {
        close(signal_fd);
        panic("poll failed");
    }

    if (fds[1].revents & POLLIN)
    {
        signalfd_siginfo info;

        if (read(signal_fd, &info, sizeof(info)) != sizeof(info))
        {
            panic("failed to read signalfd: " + std::to_string(errno));
        }

        close(signal_fd);
        return info.ssi_signo;
    }

    close(signal_fd);
    return 0;
}

void client_recv_sync(Client *client, Message *msg)
{
    // std::cout << "client_recv_sync(): waiting for message" << std::endl;

    // this is a blocking call
    // we need to wait for the message to arrive
    // and then return it

    for (;;)
    {
        if (auto sig = fd_wait(client->recv_buffer_event_fd, -1, POLLIN))
        {
            if (sig == SIGINT || sig == SIGQUIT || sig == SIGTERM)
            {
                // todo: is this correct?
                return;
            }

            panic("failed to wait on fd; signal: " + std::to_string(sig));
        }

        // decrement the semaphore
        eventfd_t value;
        if (eventfd_read(client->recv_buffer_event_fd, &value) < 0)
        {
            if (errno == EAGAIN)
            {
                continue;
            }

            panic("handle eventfd read error");
        }

        break;
    }

    while (!client->recv_buffer.try_dequeue(*msg))
    {
        std::this_thread::yield();
    }
}

void peer_destroy(Peer *peer)
{
    // std::cout << "destroying peer with fd: " << peer->fd << std::endl;

    close(peer->fd);
    free(peer->identity.data);
    delete peer;
}

void client_destroy(Client *client)
{
    std::cout << "destroying client: " << client->identity.as_string() << std::endl;

    auto session = client->session;

    // take exclusive lock on the session
    std::cout << "client_destroy(): lock session" << std::endl;
    session->mutex.lock();
    std::cout << "client_destroy(): locked session" << std::endl;
    client->mutex.lock();
    std::cout << "client_destroy(): locked client" << std::endl;

    // remove the client from the session
    std::erase(session->clients, client);

    if (client->fd > 0)
        remove_epoll_fd(session, client->fd);
    remove_epoll_fd(session, client->send_event_fd);
    remove_epoll_fd(session, client->recv_event_fd);

    for (auto peer : client->peers)
    {
        std::cout << "client_destroy(): removing peer fd: " << peer->fd << std::endl;
        remove_epoll_fd(session, peer->fd);
    }

    // we're done with the session
    session->mutex.unlock();

    std::cout << "client_destroy(): unlocked session" << std::endl;

    close(client->send_event_fd);
    close(client->recv_event_fd);
    close(client->recv_buffer_event_fd);
    close(client->unmuted_event_fd);

    if (client->fd > 0)
        close(client->fd);

    free(client->identity.data);

    for (auto peer : client->peers)
        peer_destroy(peer);

    client->peers.clear();

    while (!client->muted.empty())
    {
        auto send = client->muted.front();
        client->muted.pop();
        free(send.msg.payload.data);
    }

    client->mutex.unlock();

    std::cout << "destroyed client" << std::endl;

    // call the C++ destructor without freeing the memory
    // the memory is owned by Python
    client->~Client();
}

// this is called by python after the message's content is copied (for now)
// this is a pointer to a stack variable, so we _do not_ free it
void message_destroy(Message *msg)
{
    // this data belongs to the Client or Peer
    // free(msg->address.data);

    // always heap-allocated and owned by the message
    free(msg->payload.data);
}

// pre:
// - mutex lock
//
// post: same as pre
SendResult client_send_(Client *client, Message &&msg)
{
    // this lock guards the peers list and ensures
    // it is not mutated by another thread
    // e.g. accepting a new connection, or something else
    // std::scoped_lock lock(client->mutex);

    // std::cout << "client_send_(): send msg len: " << msg.payload.len << std::endl;

    switch (client->type)
    {
    case ConnectorType::Pair:
    {
        if (client->peers.empty())
            return SendResult::Muted;

        auto fd = client->peers[0]->fd;

        // std::cout << "client_send_(pair): send to peer: " << msg.address.as_string() << std::endl;

        if (write_message(fd, &msg.payload) == WriteResult::Disconnected)
        {
            std::cout << "client_send_(): disconnected" << std::endl;
        }
        // std::cout << "client_send_(): sent message from: " << client->identity.as_string() << "; to: " << client->peers[0]->identity.as_string() << ";" << std::string((char *)msg.address.data, msg.address.len) << ";; HASH: " << easy_hash(msg.payload.data, msg.payload.len) << std::endl;
    }
    break;
    case ConnectorType::Router:
    {
        Peer *peer;
        if (!client->peer_by_id(msg.address, &peer))
        {
            // routers drop messages
            break;
        }

        // std::cout << "client_send_(router): send to peer: " << msg.address.as_string() << std::endl;

        // write_message(peer->fd, &msg.payload);
        if (write_message(peer->fd, &msg.payload) == WriteResult::Disconnected)
        {
            std::cout << "client_send_(): disconnected" << std::endl;
        }
        // std::cout << "client_send_(): sent message from: " << client->identity.as_string() << "; to: " << peer->identity.as_string() << ";" << std::string((char *)msg.address.data, msg.address.len) << ";; HASH: " << easy_hash(msg.payload.data, msg.payload.len) << std::endl;
    }
    break;
    case ConnectorType::Pub:
    {
        // if the socket has no peers, the message is dropped
        for (auto peer : client->peers)
        {
            // std::cout << "client_send_(pub): send to peer: " << msg.address.as_string() << std::endl;

            if (write_message(peer->fd, &msg.payload) == WriteResult::Disconnected)
            {
                std::cout << "client_send_(): disconnected" << std::endl;
            }
            // write_message(peer->fd, &msg.payload);
            // std::cout << "client_send_(): sent message from: " << client->identity.as_string() << "; to: " << peer->identity.as_string() << ";" << std::string((char *)msg.address.data, msg.address.len) << ";; HASH: " << easy_hash(msg.payload.data, msg.payload.len) << std::endl;
        }
    }
    break;
    case ConnectorType::Dealer:
    {
        if (client->peers.empty())
            return SendResult::Muted;

        // dealers round-robin their peers
        auto peer = client->peers[client->rr];
        client->rr = (client->rr + 1) % client->peers.size();

        // std::cout << "client_send_(dealer): send to peer: " << msg.address.as_string() << std::endl;

        if (write_message(peer->fd, &msg.payload) == WriteResult::Disconnected)
        {
            std::cout << "client_send_(): disconnected" << std::endl;
        }

        // std::cout << "client_send_(): sent message from: " << client->identity.as_string() << "; to: " << peer->identity.as_string() << ";" << std::string((char *)msg.address.data, msg.address.len) << ";; HASH: " << easy_hash(msg.payload.data, msg.payload.len) << std::endl;
    }
    break;
    default:
        panic("unknown client type");
    }

    return SendResult::Sent;
}

const char *connector_type_to_string(ConnectorType type)
{
    switch (type)
    {
    case ConnectorType::Pair:
        return "Pair";
    case ConnectorType::Router:
        return "Router";
    case ConnectorType::Pub:
        return "Pub";
    case ConnectorType::Dealer:
        return "Dealer";
    default:
        return "Unknown";
    }
}

void client_send_sync(Client *client, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len)
{
    // std::cout << "client_send_sync(): sending message to: " << std::string((char *)to, to_len) << std::endl;

    auto session = client->session;

send:
    Message msg{
        .address = Bytes{to, to_len},
        .payload = Bytes{data, data_len},
    };

    // std::cout << "send to: " << std::string((char *)msg.address.data, msg.address.len) << std::endl;

    std::cout << "client_send_sync(): lock session" << std::endl;
    session->mutex.lock_shared();
    client->mutex.lock();
    auto res = client_send_(client, std::move(msg));
    client->mutex.unlock();
    session->mutex.unlock_shared();

    if (res == SendResult::Muted)
    {
        std::cout << "client_send_sync(): muted" << std::endl;

    wait:

        if (auto sig = fd_wait(client->unmuted_event_fd, -1, POLLIN))
            panic("failed to wait on fd; signal: " + std::to_string(sig));

        // std::cout << "client_send_sync(): woke up" << std::endl;

        eventfd_t value;
        if (eventfd_read(client->unmuted_event_fd, &value) < 0)
        {
            if (errno == EAGAIN)
                goto wait;
        }

        // std::cout << "client_send_sync(): woke up" << std::endl;

        goto send;
    }
}

// epoll handlers
void client_send_event(Client *client)
{
    for (;;)
    {
        // decrement the semaphore
        eventfd_t value;
        if (eventfd_read(client->send_event_fd, &value) < 0)
        {
            // semaphore is zero, we can epoll_wait() again
            if (errno == EAGAIN)
            {
                break;
            }

            panic("handle eventfd read error: " + std::to_string(errno));
        }

        // invariant: if we decremented the semaphore the queue must have a message
        // we loop because thread synchronization may be delayed
        SendMsg send;
        while (!client->send.try_dequeue(send))
        {
            std::this_thread::yield();
        }

        // std::cout << "client_send_event()" << std::endl;

        client->mutex.lock();
        auto res = client_send_(client, std::move(send.msg));

        switch (res)
        {
        case SendResult::Sent:
            future_set_result(send.future, NULL);
            break;
        case SendResult::Muted:
            std::cout << "client_send_event(): muted" << std::endl;
            client->muted.push(send);
        }
        client->mutex.unlock();
    }

    client->session->mutex.unlock_shared();
}

void client_recv_event(Client *client)
{
    // in this event, the recv_event_fd has proc'd
    // but we'll read the recv_buffer_event_fd first
    // to ensure that a message is available to complete the future

    for (;;)
    {
        eventfd_t value;
        if (eventfd_read(client->recv_buffer_event_fd, &value) < 0)
        {
            if (errno == EAGAIN)
            {
                // the semaphore is zero, we can epoll_wait again (edge-triggered)
                break;
            }

            panic("handle eventfd read error");
        }

        // decrement the semaphore
        if (eventfd_read(client->recv_event_fd, &value) < 0)
        {
            // the semaphore is zero, we can epoll_wait again (edge-triggered)
            if (errno == EAGAIN)
            {
                // we need to re-increment the semaphore because we didn't process the message
                if (eventfd_write(client->recv_buffer_event_fd, 1) < 0)
                {
                    panic("failed to write to eventfd: " + std::to_string(errno));
                }
                else
                {
                    // std::cout << "client_recv_event(): re-incremented recv_buffer_event_fd" << std::endl;
                }

                break;
            }

            // there aren't really any handle-able errors here
            // maybe EINTR if we are interrupted
            panic("handle eventfd read error");
        }

        // invariant: if we decrement the semaphore the queue must have a future
        void *future;
        while (!client->recv.try_dequeue(future))
        {
            std::this_thread::yield();
        }

        Message msg;
        while (!client->recv_buffer.try_dequeue(msg))
        {
            std::this_thread::yield();
        }

        // std::cout << "client_recv_event(): complete future" << std::endl;

        // just like in `client_epoll_connected_event` this is the address of a stack variable
        future_set_result(future, &msg);
    }

    client->session->mutex.unlock_shared();
}

void client_peer_recv_event(Peer *peer)
{
    // std::cout << "client_peer_recv_event(): deref peer->client ;; peer fd: " << peer->fd << std::endl;
    auto client = peer->client;
    // std::cout << "client_peer_recv_event(): deref client->session" << std::endl;
    auto session = client->session;

    std::cout << "client_peer_recv_event(): locking client->mutex" << std::endl;
    client->mutex.lock();

    for (;;)
    {
        // read message
        // if we have outstanding reads: complete them
        // if not, add to recv buffer
        // break the loop when read() returns EAGAIN or EWOULDBLOCK

        std::cout << "client_peer_recv_event(): " << peer->identity.as_string() << std::endl;

        Bytes payload;
        auto status = read_message(peer->fd, &payload, true, 3000);

        std::cout << "client_peer_recv_event(): read status: " << (int)status << std::endl;

        // this means we have exhausted the data and can epoll_wait again
        if (status == ReadResult::NoData || status == ReadResult::TimedOut)
        {
            break;
        }

        if (status == ReadResult::Disconnect)
        {
            std::cout << "peer disconnected! " << peer->identity.as_string() << std::endl;
            break;

            auto fd = peer->fd;

            // we need an exclusive lock on the session to remove epoll data
            // global ordering requires us to relinquish the client lock first
            // upgrade the lock on the session and remove the epoll
            // this is a relatively uncommon occurrence
            std::cout << "client_peer_recv_event(): unlocking client->mutex" << std::endl;
            client->mutex.unlock();
            std::cout << "client_peer_recv_event(): unlocking shared session" << std::endl;
            session->mutex.unlock_shared();
            std::cout << "client_peer_recv_event(): lock session" << std::endl;
            // session->mutex.lock();
            while (!session->mutex.try_lock())
            {
                std::cout << "client_peer_recv_event(): waiting for session lock" << std::endl;

                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            remove_epoll_fd(session, fd);
            std::cout << "client_peer_recv_event(): unlocking session" << std::endl;
            session->mutex.unlock();
            std::cout << "client_peer_recv_event(): locking shared session" << std::endl;
            // session->mutex.lock_shared();
            client->mutex.lock();

            std::cout << "client_peer_recv_event(): client locked" << std::endl;

            // remove peer from client list
            std::erase(client->peers, peer);

            // note: no locks held after this returns
            auto connected = client_connect_(client, std::move(peer->addr), 3);

            // we don't need to hold a lock to call this because we should have exclusive
            // ownership of the peer after we removed it from the client's list
            peer_destroy(peer);

            std::cout << "client_peer_recv_event(): peer destroyed" << std::endl;

            if (connected)
            {
                std::cout << "client_peer_recv_event(): reconnected" << std::endl;
            }
            else
            {
                // todo: what to do here?
                // panic("failed to reconnect");
                std::cout << "failed to reconnect to peer" << std::endl;
            }

            // calling code expects shard session lock to be held
            // session->mutex.lock_shared();

            return;
        }

        auto msg = Message{
            .address = peer->identity,
            .payload = payload,
        };

        client->recv_msg(std::move(msg));
    }

    client->mutex.unlock();
    session->mutex.unlock_shared();
}

void set_sock_opts(int fd)
{
    timeval tv{
        .tv_sec = 1,
        .tv_usec = 0};

    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    // set the TCP_NODELAY option
    // this disables Nagle's algorithm
    // which can cause latency in some cases
    // this is a trade-off for throughput
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));

    // set the TCP_QUICKACK option
    // this disables delayed ACKs
    // which can cause latency in some cases
    // this is a trade-off for throughput
    setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &on, sizeof(on));

    // set the TCP_DEFER_ACCEPT option
    // this delays the accept() call until data is available
    // this is useful for reducing the number of accept() calls
    // and can be used to reduce the number of connections
    setsockopt(fd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &on, sizeof(on));

    // set the TCP_KEEPIDLE option
    // this is the time in seconds before the first keepalive probe is sent
    // this is useful for detecting dead peers
    int keepalive = 1;
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive));

    int keepidle = 60;
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &keepidle, sizeof(keepidle));

    // set the TCP_KEEPINTVL option
    // this is the time in seconds between keepalive probes
    // this is useful for detecting dead peers
    int keepintvl = 10;
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &keepintvl, sizeof(keepintvl));

    // set the TCP_KEEPCNT option
    // this is the number of keepalive probes to send before the connection is considered dead
    // this is useful for detecting dead peers
    int keepcnt = 3;
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &keepcnt, sizeof(keepcnt));

    // set the TCP_USER_TIMEOUT option
    // this is the time in milliseconds before the connection is considered dead
    // this is useful for detecting dead peers
    int timeout = 5000;
    setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &timeout, sizeof(timeout));

    // set the TCP_LINGER2 option
    // this is the time in seconds to wait before forcibly closing the connection
    // this is useful for detecting dead peers
    linger l{
        .l_onoff = 1,
        .l_linger = 5,
    };

    setsockopt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l));

    // set the TCP_MAXSEG option
    // this is the maximum segment size for outgoing data
    // this is useful for reducing the number of packets sent
    int mss = 1460;
    setsockopt(fd, IPPROTO_TCP, TCP_MAXSEG, &mss, sizeof(mss));

    // set the TCP_CORK option
    // this is used to delay sending data until the buffer is full
    // this is useful for reducing the number of packets sent
    // setsockopt(fd, IPPROTO_TCP, TCP_CORK, &on, sizeof(on));
}

void client_listener_event(Client *client)
{
    auto session = client->session;
    auto client_fd = client->fd;

    for (;;)
    {
        sockaddr_storage addr;
        socklen_t addr_len = sizeof(addr);

        // safety: only registered to epoll once we have a listener
        auto fd = accept4(client->fd, (sockaddr *)&addr, &addr_len, SOCK_NONBLOCK);

        if (fd < 0)
        {
            // this could happen if we raced with another thread
            // also needed because of edge-triggered epoll
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                std::cout << "client_listener_event(): EAGAIN" << std::endl;

                break;
            }

            panic("todo: accept() error handling");
        }

        set_sock_opts(fd);

        // send our identity
        write_message(fd, &client->identity);

        // std::cout << "receiving identity" << std::endl;

        Bytes identity;
        auto status = read_message(fd, &identity, false, 300);

        if (status == ReadResult::Disconnect || status == ReadResult::TimedOut)
        {
            close(fd);
            // std::cout << "peer disconnected during identity exchange" << std::endl;
            break;
        }

        // std::cout << "recv identity: " << identity.len << std::endl;

        if (fd == 0)
        {
            panic("client listener: fd is 0???");
        }

        auto peer = new Peer{
            .client = client,
            .identity = identity,
            .addr = addr,
            .fd = fd};

        client->mutex.lock();
        client->peers.push_back(peer);

        // process all the messages in client->muted
        // pop each message and send
        while (!client->muted.empty())
        {
            auto send = client->muted.front();
            client->muted.pop();

            auto res = client_send_(client, std::move(send.msg));

            switch (res)
            {
            case SendResult::Sent:
                future_set_result(send.future, NULL);
                break;
            case SendResult::Muted:
                panic("muted after reconnect");
            }
        }

        if (eventfd_write(client->unmuted_event_fd, 1) < 0)
        {
            panic("failed to write to eventfd: " + std::to_string(errno));
        }

        // yep, that's a lot of mutex ops
        // unfortunately global ordering requires it
        // std::cout << "client_listener_event(): unlocking client->mutex" << std::endl;
        client->mutex.unlock();
        // std::cout << "client_listener_event(): unlocking shared session" << std::endl;
        session->mutex.unlock_shared();
        std::cout << "client_listener_event(): lock session" << std::endl;
        session->mutex.lock();
        std::cout << "client_listener_event(): locked session" << std::endl;

        // we need to check that the client is still in the session
        // it's possible that the client was destroyed while we were upgrading the lock
        if (!has_epoll_data_fd(session, client_fd))
        {
            std::cout << "client_listener_event(): pre-empted" << std::endl;

            // the caller is expecting the shared lock to be held
            session->mutex.unlock();
            std::cout << "client_listener_event(): lock session" << std::endl;
            // session->mutex.lock_shared();
            break;
        }

        std::cout << "add client peer recv: " << fd << " for client: " << client->identity.as_string() << std::endl;
        add_epoll_fd(session, fd, EpollType::ClientPeerRecv, peer);

        // downgrade the lock
        // std::cout << "client_listener_event(): unlocking session" << std::endl;
        session->mutex.unlock();
        // std::cout << "client_listener_event(): locking shared session" << std::endl;
        std::cout << "client_listener_event(): lock session" << std::endl;
        session->mutex.lock_shared();

        std::cout << "accepted peer: " << identity.as_string() << std::endl;
    }

    session->mutex.unlock_shared();
}

// lock-free
void Client::recv_msg(Message &&msg)
{
    // std::cout << "Client::recv_msg(): recv: " << identity.as_string() << " ; from: " << msg.address.as_string() << " ; HASH: " << easy_hash(msg.payload.data, msg.payload.len) << std::endl;

    // try to dequeue a future from the recv queue
    // for sync clients this will never happen
    eventfd_t value;
    if (eventfd_read(this->recv_event_fd, &value) == 0)
    {
        // std::cout << "optimistically completing recv; recv count: " << value << std::endl;

        void *future;
        while (!this->recv.try_dequeue(future))
        {
            std::this_thread::yield();
        }

        // std::cout << "dq'd future for completion" << std::endl;

        // // std::cout << "msg len: " << msg.payload.len << std::endl;

        future_set_result(future, &msg);
    }
    else
    {
        if (errno != EAGAIN)
        {
            panic("eventfd read error: " + std::to_string(errno) + "; " + strerror(errno));
        }

        // std::cout << "enqueuing msg with len: " << msg.payload.len << std::endl;

        // no outstanding recvs, buffer the message
        this->recv_buffer.enqueue(msg);

        if (eventfd_write(this->recv_buffer_event_fd, 1) < 0)
        {
            panic("failed to write to eventfd: " + std::to_string(errno));
        }
    }
}

void intraprocess_bind(IntraProcessClient *inproc, const char *addr, size_t len)
{
    // exclusive lock
    std::unique_lock lock(inproc->session->intraprocess_mutex);

    inproc->bind = std::string(addr, len);

    for (
        size_t i = 0; i < inproc->session->inprocs.size(); i++)
    {
        if (i == inproc->id)
        {
            continue;
        }

        auto peer = inproc->session->inprocs[i];

        if (peer->bind == inproc->bind)
        {
            // todo error handling
            panic("inproc bind conflict");
        }

        if (peer->connecting == inproc->bind)
        {
            peer->connecting = std::nullopt;
            peer->peer = inproc->id;
            inproc->peer = i;

            // pair sockets
            break;
        }
    }
}

void intraprocess_connect(IntraProcessClient *inproc, const char *addr, size_t len)
{
    // exclusive lock
    std::unique_lock lock(inproc->session->intraprocess_mutex);

    inproc->connecting = std::string(addr, len);

    for (
        size_t i = 0; i < inproc->session->inprocs.size(); i++)
    {
        auto peer = inproc->session->inprocs[i];

        if (peer->bind == inproc->connecting)
        {
            peer->peer = inproc->id;
            inproc->connecting = std::nullopt;
            inproc->peer = i;

            // pair sockets
            break;
        }
    }
}

void intraprocess_send(IntraProcessClient *inproc, uint8_t *data, size_t len)
{
    // shared lock
    std::shared_lock lock(inproc->session->intraprocess_mutex);

    if (!inproc->peer)
    {
        // todo error handling
        panic("inproc socket not connected");
    }

    uint8_t *data_dup = (uint8_t *)malloc(len * sizeof(uint8_t));
    memcpy(data_dup, data, len);

    auto peer = inproc->session->inprocs[*inproc->peer];

    peer->queue.enqueue(Message{
        .address = inproc->identity,
        .payload = Bytes{data_dup, len},
    });

    // std::cout << "inproc sent: posting semaphore" << std::endl;

    if (eventfd_write(peer->recv_buffer_event_fd, 1) < 0)
    {
        panic("failed to write to eventfd: " + std::to_string(errno));
    }
}

void intraprocess_recv_sync(IntraProcessClient *inproc, Message *msg)
{
    for (;;)
    {
        // std::cout << "inproc recv sync: going to wait on eventfd" << std::endl;

        if (auto status = fd_wait(inproc->recv_buffer_event_fd, -1, POLLIN))
        {
            panic("signal received: " + std::string(strsignal(status)));
        }

        // std::cout << "inproc recv sync: eventfd signaled" << std::endl;

        // decrement the semaphore
        eventfd_t value;
        if (eventfd_read(inproc->recv_buffer_event_fd, &value) < 0)
        {
            if (errno == EAGAIN)
            {
                continue;
            }

            panic("handle eventfd read error");
        }

        // std::cout << "inproc recv sync: dequeuing message" << std::endl;

        while (!inproc->queue.try_dequeue(*msg))
        {
            std::this_thread::yield();
        }

        return;
    }
}

void intraprocess_recv_async(void *future, IntraProcessClient *inproc)
{
    inproc->recv.enqueue(future);

    if (eventfd_write(inproc->recv_event_fd, 1) < 0)
    {
        panic("failed to write to eventfd: " + std::to_string(errno));
    }
}

void intraprocess_init(Session *session, IntraProcessClient *inproc, uint8_t *identity, size_t len)
{
    uint8_t *identity_dup = (uint8_t *)malloc(len * sizeof(uint8_t));
    memcpy(identity_dup, identity, len);

    auto id = session->id_counter++;

    new (inproc) IntraProcessClient{
        .id = id,
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

    session->intraprocess_mutex.lock();
    session->inprocs.push_back(inproc);
    session->intraprocess_mutex.unlock();

    std::cout << "intraprocess_init(): lock session" << std::endl;
    session->mutex.lock();
    add_epoll_fd(session, inproc->recv_event_fd, EpollType::IntraProcessClientRecv, inproc);
    session->mutex.unlock();
}

void intraprocess_recv_event(IntraProcessClient *inproc)
{
    for (;;)
    {
        // shared lock
        // std::cout << "A!" << std::endl;

        eventfd_t value;
        if (eventfd_read(inproc->recv_buffer_event_fd, &value) < 0)
        {
            // this means the recv buffer is empty

            // the semaphore is zero, we can epoll_wait again (edge-triggered)
            if (errno == EAGAIN)
            {
                break;
            }

            panic("handle eventfd read error");
        }

        if (eventfd_read(inproc->recv_event_fd, &value) < 0)
        {
            // eventfd_write(inproc->recv_buffer_event_fd, 1);

            // this means the recv buffer is empty

            // the semaphore is zero, we can epoll_wait again (edge-triggered)
            if (errno == EAGAIN)
            {
                // we need to write back to the recv_buffer_event_fd
                // because we didn't process the message
                if (eventfd_write(inproc->recv_buffer_event_fd, 1) < 0)
                {
                    panic("failed to write to eventfd: " + std::to_string(errno));
                }
                else
                {
                    // std::cout << "inproc_recv_event(): re-incremented recv_buffer_event_fd" << std::endl;
                }

                break;
            }

            panic("handle eventfd read error");
        }

        void *future;
        while (!inproc->recv.try_dequeue(future))
        {
            std::this_thread::yield();
        }

        Message msg;
        while (!inproc->queue.try_dequeue(msg))
        {
            std::this_thread::yield();
        }

        // std::cout << "msg len: " << msg.payload.len << std::endl;

        future_set_result(future, &msg);
    }

    inproc->session->mutex.unlock_shared();
}
