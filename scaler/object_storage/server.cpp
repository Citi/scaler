
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
#include <optional>

#include "defs.h"
#include "io_helper.h"
#include "object_instruction_header.capnp.h"

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;
namespace this_coro = boost::asio::this_coro;

#if defined(BOOST_ASIO_ENABLE_HANDLER_TRACKING)
#define use_awaitable boost::asio::use_awaitable_t(__FILE__, __LINE__, __PRETTY_FUNCTION__)
#endif

std::map<object_id_t, object_with_meta> object_id_to_meta;

void update_record(object_header& header, payload_t payload) {
    using type = ObjectInstructionHeader::ObjectInstructionType;
    switch (header.ins_type) {
        case type::SET_OBJECT_CONTENT_BY_ID:
            object_id_to_meta[header.object_id].object = std::make_shared<object_t>(std::move(payload));
            break;

        case type::SET_OBJECT_NAME_BY_ID: object_id_to_meta[header.object_id].name = std::move(payload); break;

        case type::DEL_OBJECT_BY_NAME:
            for (const auto& [obj_id, meta]: object_id_to_meta) {
                if (meta.name == payload) {
                    header.object_id = obj_id;
                    object_id_to_meta.erase(obj_id);
                    return;
                }
            }
            header.object_id = {0, 0, 0, 0};
            break;

        case type::DEL_OBJECT_BY_ID: object_id_to_meta.erase(header.object_id); break;

        case type::GET_OBJECT_CONTENT_BY_ID:
            header.payload_length = object_id_to_meta[header.object_id].object->size();
            break;
        case type::GET_OBJECT_NAME_BY_ID:
            header.payload_length = object_id_to_meta[header.object_id]
                                        .name                                                             //
                                        .and_then([](const auto& x) { return std::optional(x.size()); })  //
                                        .value_or(0uz);                                                   //
            break;
        default: break;
    }
}

std::span<const unsigned char> get_memory_view_for_response_payload(object_header& header) {
    using type = ObjectInstructionHeader::ObjectInstructionType;
    switch (header.ins_type) {
        case type::GET_OBJECT_CONTENT_BY_ID:
            return {
                object_id_to_meta[header.object_id].object->data(), object_id_to_meta[header.object_id].object->size()};
        case type::GET_OBJECT_NAME_BY_ID:
            if (!(object_id_to_meta[header.object_id].name))
                break;
            return {object_id_to_meta[header.object_id].name->data(), object_id_to_meta[header.object_id].name->size()};

        case type::SET_OBJECT_CONTENT_BY_ID:
        case type::SET_OBJECT_NAME_BY_ID:
        case type::DEL_OBJECT_BY_NAME:
        case type::DEL_OBJECT_BY_ID:
        default: break;
    }
    return {static_cast<const unsigned char*>(nullptr), 0};
}

awaitable<void> process_request(tcp::socket socket) {
    try {
        for (;;) {
            object_header header;
            co_await read_request_header(socket, header);

            payload_t payload;
            co_await read_request_payload(socket, header, payload);

            update_record(header, std::move(payload));

            auto payload_view = get_memory_view_for_response_payload(header);

            co_await write_response_header(socket, header, payload_view.size());

            co_await write_response_payload(socket, payload_view);
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
    short unsigned port = 55555;
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
