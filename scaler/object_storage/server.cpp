#include "server.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>
#include <cstdio>
#include <string>
#include <sys/eventfd.h>

#include "object_storage_server.h"

// Helper macros to stringify the macro value
#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x)        STRINGIFY_HELPER(x)

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;
namespace this_coro = boost::asio::this_coro;

#if defined(BOOST_ASIO_ENABLE_HANDLER_TRACKING)
#define use_awaitable boost::asio::use_awaitable_t(__FILE__, __LINE__, __PRETTY_FUNCTION__)
#endif

scaler::object_storage::ObjectStorageServer server;

int create_server_ready_eventfd() {
    int on_server_ready_fd = eventfd(0, EFD_SEMAPHORE);
    if (on_server_ready_fd == -1) {
        std::cerr << "create on_server_ready_fd failed: errno=" << errno << std::endl;
        std::terminate();
    }

    return on_server_ready_fd;
}

void wait_server_ready_eventfd(int on_server_ready_fd) {
    if (on_server_ready_fd < 0) {
        return;
    }

    uint64_t value;
    ssize_t ret = read(on_server_ready_fd, &value, sizeof (uint64_t));

    if (ret != sizeof (uint64_t)) {
        std::cerr << "read from on_server_ready_fd failed: errno=" << errno << std::endl;
        std::terminate();
    }
}

void set_server_ready_eventfd(int on_server_ready_fd) {
    if (on_server_ready_fd < 0) {
        return;
    }

    uint64_t value = 1;
    ssize_t ret = write(on_server_ready_fd, &value, sizeof (uint64_t));

    if (ret != sizeof (uint64_t)) {
        std::cerr << "write to on_server_ready_fd failed: errno=" << errno << std::endl;
        std::terminate();
    }
}

void set_tcp_no_delay(tcp::socket& socket, bool is_no_delay) {
    boost::system::error_code ec;
    socket.set_option(tcp::no_delay(is_no_delay), ec);

    if (ec) {
        std::cerr << "failed to set TCP_NODELAY on client socket: " << ec.message() << std::endl;
        std::terminate();
    }
}

awaitable<void> listener(boost::asio::ip::tcp::endpoint endpoint, int on_server_ready_fd) {
    auto executor = co_await this_coro::executor;
    tcp::acceptor acceptor(executor, endpoint);

    set_server_ready_eventfd(on_server_ready_fd);

    for (;;) {
        auto shared_socket = std::make_shared<tcp::socket>(executor);
        co_await acceptor.async_accept(*shared_socket, use_awaitable);
        set_tcp_no_delay(*shared_socket, true);

        co_spawn(executor, server.process_request(std::move(shared_socket)), detached);
    }
}

// Assuming name_ and port_ is valid name and port
// TODO: In the future we might need to add expiration date for closing connections
void run_object_storage_server(const char* name_, const char* port_, int on_server_ready_fd) {
    std::string name = name_;
    std::string port = port_;

    try {
        boost::asio::io_context io_context(1);
        tcp::resolver resolver(io_context);
        auto res = resolver.resolve(name, port);

        boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) { io_context.stop(); });

        co_spawn(io_context, listener(res.begin()->endpoint(), on_server_ready_fd), detached);

        io_context.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        std::cerr << "Mostly something serious happen, inspect capnp header corruption" << std::endl;
    }
}
