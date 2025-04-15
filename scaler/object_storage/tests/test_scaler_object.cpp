#include <capnp/message.h>
#include <capnp/serialize.h>
#include <gtest/gtest.h>
#include <string.h>

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <span>

#include "../constants.h"
#include "../defs.h"
#include "../io_helper.h"
#include "protocol/object_instruction_header.capnp.h"

using boost::asio::awaitable;
using boost::asio::buffer;
using boost::asio::ip::tcp;
using type = ObjectInstructionHeader::ObjectInstructionType;

char payload[] = "Hello, world!";

awaitable<void> testSetObjectByID(tcp::socket& socket) {
    object_header header;
    header.ins_type       = type::SET_OBJECT_BY_I_D;
    header.payload_length = sizeof(payload);
    header.object_id      = {0, 1, 2, 3};

    // TODO: We should really rename these functions
    // Req & Resp are in terms of server behavior
    co_await write_response_header(socket, header, header.payload_length);
    co_await write_response_payload(socket, {(const unsigned char*)payload, sizeof(payload)});
    co_await read_request_header(socket, header);

    // we see the ins_type is feed back as is
    EXPECT_EQ(header.ins_type, type::SET_OBJECT_BY_I_D);
    // Current Impl makes payload_length equal to 0, but this is undefined
    EXPECT_EQ(header.payload_length, 0);
    // object_id is feed back as is
    EXPECT_EQ(header.object_id[0], 0);
    EXPECT_EQ(header.object_id[1], 1);
    EXPECT_EQ(header.object_id[2], 2);
    EXPECT_EQ(header.object_id[3], 3);
}

awaitable<void> testGetObjectByID(tcp::socket& socket) {
    object_header header;
    header.ins_type       = type::GET_OBJECT_BY_I_D;
    header.payload_length = 0;
    header.object_id      = {0, 1, 2, 3};

    co_await write_response_header(socket, header, header.payload_length);
    co_await read_request_header(socket, header);

    EXPECT_EQ(header.object_id[0], 0);
    EXPECT_EQ(header.object_id[1], 1);
    EXPECT_EQ(header.object_id[2], 2);
    EXPECT_EQ(header.object_id[3], 3);
    // we see the ins_type is feed back as is
    EXPECT_EQ(header.ins_type, type::GET_OBJECT_BY_I_D);
    // // In the previous call, we set this object_id to be payload
    EXPECT_EQ(header.payload_length, sizeof(payload));
    // // object_id is feed back as is

    payload_t buf(sizeof(payload));
    co_await read_request_payload(socket, header, buf);
    EXPECT_EQ(strncmp((const char*)buf.data(), (const char*)payload, sizeof(payload)), 0);
}

TEST(ObjectStorageTestSuite, TestHeader) {
    boost::asio::io_context io_context;
    tcp::socket socket(io_context);
    tcp::resolver resolver(io_context);
    boost::asio::connect(socket, resolver.resolve("127.0.0.1", "55555"));

    co_spawn(io_context.get_executor(), testSetObjectByID(socket), boost::asio::use_awaitable);
    co_spawn(io_context.get_executor(), testGetObjectByID(socket), boost::asio::use_awaitable);

    io_context.run();
    boost::system::error_code ec;
    auto res = socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    (void)res;
    socket.close();
}
