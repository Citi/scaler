
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

#define dprint(fmt, ...)

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

bool Session::epoll_data_by_fd(int fd, EpollData **data)
{
    auto x = std::find_if(this->epoll_data.begin(), this->epoll_data.end(), [fd](EpollData &d)
                          { return d.fd == fd; });

    if (x != this->epoll_data.end())
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
        epoll_event event;
        auto n_events = epoll_wait(session->epoll_fd, &event, 1, -1);

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
        // std::cout << "io-thread[" << id << "]: locking session: " << event.data.fd << std::endl;
        session->mutex.lock_shared();

        // note, the epoll data will only be valid while the shared lock is held
        EpollData *data;
        if (!session->epoll_data_by_fd(event.data.fd, &data))
        {
            session->mutex.unlock_shared();
            continue;
        }

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
    session->add_epoll_fd(session->epoll_close_efd, EpollType::EpollClosed, NULL, false);

    session->threads.reserve(num_threads);

    for (size_t i = 0; i < num_threads; ++i)
    {
        session->threads.emplace_back(
            std::thread(io_thread_main, session, i));
    }
}

void session_destroy(Session *session)
{
    // this will wake up the io threads and cause them to exit
    if (eventfd_write(session->epoll_close_efd, 1) < 0)
    {
        panic("failed to write to epoll close eventfd");
    }

    // wait for all threads to exit
    for (size_t i = 0; i < session->threads.size(); ++i)
    {
        // std::cout << "joining thread " << i << std::endl;
        session->threads[i].join();
    }

    close(session->epoll_fd);

    // call the destructor without freeing the memory (it's owned by the caller)
    session->~Session();
}

bool Session::has_epoll_data_fd(int fd)
{
    for (auto &d : this->epoll_data)
    {
        if (d.fd == fd)
        {
            return true;
        }
    }

    return false;
}

void Session::add_epoll_fd(int fd, EpollType type, void *data, bool edge_triggered)
{
    EpollData epoll_data{
        .fd = fd,
        .type = type,
        .ptr = data,
    };

    if (has_epoll_data_fd(fd))
    {
        panic("epoll fd already exists: " + std::to_string(fd));
    }

    this->epoll_data.push_back(epoll_data);

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

    if (epoll_ctl(this->epoll_fd, EPOLL_CTL_ADD, fd, &event) < 0)
    {
        panic("failed to add epoll fd: " + std::to_string(fd) + "; " + strerror(errno));
    }
}

// must hold exclusive lock on session
void Session::remove_epoll_fd(int fd)
{
    if (epoll_ctl(this->epoll_fd, EPOLL_CTL_DEL, fd, nullptr) < 0)
    {
        // we ignore enoent because it means the fd was already removed
        if (errno != ENOENT)
            panic("failed to remove epoll fd: " + std::to_string(fd) + "; " + strerror(errno));
    }

    std::erase_if(this->epoll_data, [fd](EpollData &data)
                  { return data.fd == fd; });
}

void client_init(Session *session, Client *client, Transport transport, uint8_t *identity, size_t len, ConnectorType type)
{
    // todo: error handling?
    if (transport == Transport::IntraProcess)
    {
        panic("IntraProcess transport has a separate API");
    }

    new (client) Client{
        .type = type,
        .transport = transport,

        .mutex = std::mutex(),
        .session = session,

        // *identity is owned by the caller, we need our own copy
        .identity = Bytes{
            .data = datadup(identity, len),
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
        .recv_buffer_event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE),
        .recv_buffer = ConcurrentQueue<Message>(),
    };

    session->mutex.lock();
    session->clients.push_back(client);
    session->add_epoll_fd(client->send_event_fd, EpollType::ClientSend, client);
    session->add_epoll_fd(client->recv_event_fd, EpollType::ClientRecv, client);
    session->mutex.unlock();
}

void client_bind(Client *client, const char *host, uint16_t port)
{
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

    // todo: replace this block with a switch ovre the transport
    {
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
    // std::cout << "client_bind(): lock session" << std::endl;
    session->mutex.lock();
    session->add_epoll_fd(client->fd, EpollType::ClientListener, client);
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

    session->mutex.lock();
    session->add_epoll_fd(peer->fd, EpollType::ClientPeerRecv, peer);
    session->mutex.unlock();
    return true;
}

void client_connect(Client *client, const char *host, uint16_t port)
{
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

    client->send.enqueue(send);

    // notify io threads
    if (eventfd_write(client->send_event_fd, 1) < 0)
    {
        panic("failed to write to eventfd: " + std::to_string(errno));
    }
}

void client_recv(void *future, Client *client)
{
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
    close(peer->fd);
    free(peer->identity.data);
    delete peer;
}

void client_destroy(Client *client)
{
    auto session = client->session;

    // take exclusive lock on the session
    session->mutex.lock();
    client->mutex.lock();

    // remove the client from the session
    std::erase(session->clients, client);

    if (client->fd > 0)
        session->remove_epoll_fd(client->fd);
    session->remove_epoll_fd(client->send_event_fd);
    session->remove_epoll_fd(client->recv_event_fd);

    for (auto peer : client->peers)
    {
        session->remove_epoll_fd(peer->fd);
    }

    // we're done with the session
    session->mutex.unlock();

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

    // call the C++ destructor without freeing the memory
    // the memory is owned by Python
    client->~Client();
}

// this is called by python after the message's content is copied (for now)
// this is a pointer to a stack variable, so we _do not_ free it
void message_destroy([[maybe_unused]] Message *msg)
{
    panic("todo: remove message_destroy()");
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

    switch (client->type)
    {
    case ConnectorType::Pair:
    {
        if (client->peers.empty())
            return SendResult::Muted;

        auto fd = client->peers[0]->fd;

        if (write_message(fd, &msg.payload) == WriteResult::Disconnected)
        {
            // todo
        }
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
        if (write_message(peer->fd, &msg.payload) == WriteResult::Disconnected)
        {
            // todo
        }
    }
    break;
    case ConnectorType::Pub:
    {
        // if the socket has no peers, the message is dropped
        for (auto peer : client->peers)
        {
            if (write_message(peer->fd, &msg.payload) == WriteResult::Disconnected)
            {
                // todo
            }
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

        if (write_message(peer->fd, &msg.payload) == WriteResult::Disconnected)
        {
            // todo
        }
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
    auto session = client->session;

send:
    Message msg{
        .address = Bytes{to, to_len},
        .payload = Bytes{data, data_len},
    };

    session->mutex.lock_shared();
    client->mutex.lock();
    auto res = client_send_(client, std::move(msg));
    client->mutex.unlock();
    session->mutex.unlock_shared();

    if (res == SendResult::Muted)
    {
    wait:

        if (auto sig = fd_wait(client->unmuted_event_fd, -1, POLLIN))
            panic("failed to wait on fd; signal: " + std::to_string(sig));

        eventfd_t value;
        if (eventfd_read(client->unmuted_event_fd, &value) < 0)
        {
            if (errno == EAGAIN)
                goto wait;
        }

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

        client->mutex.lock();
        auto res = client_send_(client, std::move(send.msg));

        switch (res)
        {
        case SendResult::Sent:
            future_set_result(send.future, NULL);
            break;
        case SendResult::Muted:
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
                    panic("failed to write to eventfd: " + std::to_string(errno));

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

        // just like in `client_epoll_connected_event` this is the address of a stack variable
        future_set_result(future, &msg);
    }

    client->session->mutex.unlock_shared();
}

void client_peer_recv_event(Peer *peer)
{
    auto client = peer->client;
    auto session = client->session;

    // we need to make sure no other thread is working on this peer
    // we could maybe have a per-peer lock, but it may not be necessary
    client->mutex.lock();

    for (;;)
    {
        // read message
        // if we have outstanding reads: complete them
        // if not, add to recv buffer
        // break the loop when read() returns EAGAIN or EWOULDBLOCK

        Bytes payload;
        auto status = read_message(peer->fd, &payload, true, 3000);

        // this means we have exhausted the data and can epoll_wait again
        if (status == ReadResult::NoData || status == ReadResult::TimedOut)
            break;

        if (status == ReadResult::Disconnect)
        {
            // std::cout << "peer disconnected! " << peer->identity.as_string() << std::endl;
            break;

            // todo
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
                break;

            panic("todo: accept() error handling");
        }

        set_sock_opts(fd);

        // send our identity
        write_message(fd, &client->identity);

        Bytes identity;
        auto status = read_message(fd, &identity, false, 300);

        if (status == ReadResult::Disconnect || status == ReadResult::TimedOut)
        {
            close(fd);
            break;
        }

        if (fd == 0)
            panic("client listener: fd is 0???");

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
        client->mutex.unlock();
        session->mutex.unlock_shared();
        session->mutex.lock();
        // we need to check that the client is still in the session
        // it's possible that the client was destroyed while we were upgrading the lock
        if (!session->has_epoll_data_fd(client_fd))
        {
            // the caller is expecting the shared lock to be held
            session->mutex.unlock();
            break;
        }

        session->add_epoll_fd(fd, EpollType::ClientPeerRecv, peer);

        // downgrade the lock
        session->mutex.unlock();
        session->mutex.lock_shared();
    }

    session->mutex.unlock_shared();
}

// lock-free
void Client::recv_msg(Message &&msg)
{
    // std::cout << "recv message from: " << msg.address.as_string() << std::endl;

    // try to dequeue a future from the recv queue
    // for sync clients this will never happen
    eventfd_t value;
    if (eventfd_read(this->recv_event_fd, &value) == 0)
    {
        void *future;
        while (!this->recv.try_dequeue(future))
            std::this_thread::yield();

        future_set_result(future, &msg);
    }
    else
    {
        if (errno != EAGAIN)
            panic("eventfd read error: " + std::to_string(errno) + "; " + strerror(errno));

        // no outstanding recvs, buffer the message
        this->recv_buffer.enqueue(msg);

        if (eventfd_write(this->recv_buffer_event_fd, 1) < 0)
            panic("failed to write to eventfd: " + std::to_string(errno));
    }
}

void intraprocess_bind(IntraProcessClient *inproc, const char *addr, size_t len)
{
    // exclusive lock
    std::unique_lock lock(inproc->session->intraprocess_mutex);

    inproc->bind = std::string(addr, len);

    for (size_t i = 0; i < inproc->session->inprocs.size(); i++)
    {
        if (i == inproc->id)
            continue;

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

    if (eventfd_write(peer->recv_buffer_event_fd, 1) < 0)
    {
        panic("failed to write to eventfd: " + std::to_string(errno));
    }
}

void intraprocess_recv_sync(IntraProcessClient *inproc, Message *msg)
{
    for (;;)
    {
        if (auto status = fd_wait(inproc->recv_buffer_event_fd, -1, POLLIN))
            panic("signal received: " + std::string(strsignal(status)));

        // decrement the semaphore
        eventfd_t value;
        if (eventfd_read(inproc->recv_buffer_event_fd, &value) < 0)
        {
            if (errno == EAGAIN)
                continue;

            panic("handle eventfd read error");
        }

        while (!inproc->queue.try_dequeue(*msg))
            std::this_thread::yield();

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

    session->mutex.lock();
    session->add_epoll_fd(inproc->recv_event_fd, EpollType::IntraProcessClientRecv, inproc);
    session->mutex.unlock();
}

void intraprocess_recv_event(IntraProcessClient *inproc)
{
    for (;;)
    {
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
            // this means the recv buffer is empty

            // the semaphore is zero, we can epoll_wait again (edge-triggered)
            if (errno == EAGAIN)
            {
                // we need to write back to the recv_buffer_event_fd
                // because we didn't process the message
                if (eventfd_write(inproc->recv_buffer_event_fd, 1) < 0)
                    panic("failed to write to eventfd: " + std::to_string(errno));

                break;
            }

            panic("handle eventfd read error");
        }

        void *future;
        while (!inproc->recv.try_dequeue(future))
            std::this_thread::yield();

        Message msg;
        while (!inproc->queue.try_dequeue(msg))
            std::this_thread::yield();

        future_set_result(future, &msg);
    }

    inproc->session->mutex.unlock_shared();
}
