
#include <capnp/common.h>
#include <capnp/message.h>
#include <capnp/serialize.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/system/system_error.hpp>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>

#include "argparse/argparse.hpp"
#include "defs.h"
#include "io_helper.h"
#include "protocol/object_storage.capnp.h"
#include "version.h"
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

std::map<scaler::object_storage::object_id_t, scaler::object_storage::object_with_meta> object_id_to_meta;

awaitable<void> write_once(scaler::object_storage::meta meta);

std::span<const unsigned char> get_memory_view_for_response_payload(
    scaler::object_storage::ObjectResponseHeader& header) {
    using type = ::ObjectResponseHeader::ObjectResponseType;
    switch (header.resp_type) {
        case type::GET_O_K: return {object_id_to_meta[header.object_id].object->data(), header.payload_length};
        case type::SET_O_K:
        case type::SET_O_K_OVERRIDE:
        case type::DEL_O_K:
        case type::DEL_NOT_EXISTS:
        default: break;
    }
    return {static_cast<const unsigned char*>(nullptr), 0};
}

bool update_record(
    const scaler::object_storage::ObjectRequestHeader& request_header,
    scaler::object_storage::ObjectResponseHeader& response_header,
    scaler::object_storage::payload_t payload) {
    using req_type            = ::ObjectRequestHeader::ObjectRequestType;
    using resp_type           = ::ObjectResponseHeader::ObjectResponseType;
    response_header.object_id = request_header.object_id;
    switch (request_header.req_type) {
        case req_type::SET_OBJECT:
            response_header.resp_type =
                object_id_to_meta[request_header.object_id].object ? resp_type::SET_O_K_OVERRIDE : resp_type::SET_O_K;
            object_id_to_meta[request_header.object_id].object =
                std::make_shared<scaler::object_storage::object_t>(std::move(payload));
            for (auto& curr_meta: object_id_to_meta[request_header.object_id].meta_info) {
                auto executor = curr_meta.socket.get_executor();
                co_spawn(executor, write_once(std::move(curr_meta)), detached);
            }

            break;

        case req_type::GET_OBJECT:
            response_header.resp_type = resp_type::GET_O_K;
            if (object_id_to_meta[request_header.object_id].object)
                response_header.payload_length = object_id_to_meta[request_header.object_id].object->size();
            else
                return false;
            break;

        case req_type::GET_OBJECT_HEADER:
            response_header.resp_type = resp_type::GET_O_K;
            if (object_id_to_meta[request_header.object_id].object)
                response_header.payload_length =
                    std::min(request_header.payload_length, object_id_to_meta[request_header.object_id].object->size());
            else
                return false;
            break;

        case req_type::DELETE_OBJECT:
            response_header.resp_type =
                object_id_to_meta[request_header.object_id].object ? resp_type::DEL_O_K : resp_type::DEL_NOT_EXISTS;
            object_id_to_meta.erase(request_header.object_id);
            break;
    }
    return true;
}

awaitable<void> process_request(tcp::socket socket) {
    try {
        for (;;) {
            scaler::object_storage::ObjectRequestHeader requestHeader;
            co_await scaler::object_storage::read_request_header(socket, requestHeader);

            scaler::object_storage::payload_t payload;
            co_await scaler::object_storage::read_request_payload(socket, requestHeader, payload);

            scaler::object_storage::ObjectResponseHeader responseHeader;
            bool good_to_send = update_record(requestHeader, responseHeader, std::move(payload));

            if (!good_to_send) {
                object_id_to_meta[requestHeader.object_id].meta_info.emplace_back(
                    std::move(socket), std::move(requestHeader), std::move(responseHeader));
                break;
            }

            auto payload_view = get_memory_view_for_response_payload(responseHeader);

            co_await scaler::object_storage::write_response_header(socket, responseHeader, payload_view.size());

            co_await scaler::object_storage::write_response_payload(socket, payload_view);
        }
    } catch (std::exception& e) {
        // std::printf("process_request Exception: %s\n", e.what());
    }
}

awaitable<void> listener(short unsigned port) {
    auto executor = co_await this_coro::executor;
    tcp::acceptor acceptor(executor, {tcp::v4(), port});
    for (;;) {
        tcp::socket socket = co_await acceptor.async_accept(use_awaitable);
        co_spawn(executor, process_request(std::move(socket)), detached);
    }
}

int main(int argc, char* argv[]) {
    argparse::ArgumentParser argParser("server", STRINGIFY(VERSION));
    argParser.add_argument("-p", "--port").default_value(55555).help("Specify port to listen on.");
    try {
        argParser.parse_args(argc, argv);
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << argParser;
        std::exit(1);
    }

    unsigned short port = argParser.get<unsigned short>("--port");

    if (argc == 2)
        port = atoi(argv[1]);
    try {
        boost::asio::io_context io_context(1);

        boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) { io_context.stop(); });

        co_spawn(io_context, listener(port), detached);

        io_context.run();
    } catch (std::exception& e) { std::printf("Exception: %s\n", e.what()); }
}

awaitable<void> write_once(scaler::object_storage::meta meta) {
    using type = ::ObjectRequestHeader::ObjectRequestType;
    if (meta.request_header.req_type == type::GET_OBJECT_HEADER) {
        meta.response_header.payload_length = std::min(
            object_id_to_meta[meta.response_header.object_id].object->size(), meta.request_header.payload_length);
    } else if (meta.request_header.req_type == type::GET_OBJECT) {
        meta.response_header.payload_length = object_id_to_meta[meta.response_header.object_id].object->size();
    }

    auto payload_view = get_memory_view_for_response_payload(meta.response_header);
    co_await scaler::object_storage::write_response_header(meta.socket, meta.response_header, payload_view.size());
    co_await scaler::object_storage::write_response_payload(meta.socket, payload_view);

    co_spawn(meta.socket.get_executor(), process_request(std::move(meta.socket)), detached);
}
