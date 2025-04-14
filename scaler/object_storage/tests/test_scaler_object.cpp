#include <capnp/message.h>
#include <capnp/serialize.h>
#include <gtest/gtest.h>

#include <boost/asio.hpp>

#include "../constants.h"
#include "../defs.h"
#include "object_instruction_header.capnp.h"

using boost::asio::ip::tcp;

#define WRITE_HEADER(msg)                                                                                       \
    do {                                                                                                        \
        auto header = capnp::messageToFlatArray(msg);                                                           \
        auto result =                                                                                           \
            boost::asio::write(socket, boost::asio::buffer(header.asBytes().begin(), header.asBytes().size())); \
        /* EXPECT_EQ(0, 1); */                                                                                  \
    } while (0)

static constexpr size_t header_size = 72;
static constexpr size_t word_size   = sizeof(capnp::word);

alignas(8) char resp_header[header_size];

TEST(ObjectStorageTestSuite, TestHeader) {
    boost::asio::io_context io_context;
    // we need a socket and a resolver
    tcp::socket socket(io_context);
    tcp::resolver resolver(io_context);
    // now we can use connect(..)
    boost::asio::connect(socket, resolver.resolve("127.0.0.1", "55555"));

    // msg
    capnp::MallocMessageBuilder msg;
    auto this_msg = msg.initRoot<ObjectInstructionHeader>();

    auto obj_id_builder = this_msg.initObjectId();
    obj_id_builder.setField0(0);
    obj_id_builder.setField1(1);
    obj_id_builder.setField2(2);
    obj_id_builder.setField3(3);

    using type = ::ObjectInstructionHeader::ObjectInstructionType;

    ///////////////////////////////////////////////////////////////////
    this_msg.setInstruction(type::SET_OBJECT_CONTENT_BY_ID);

    char payload[] = "Hello, world!";
    this_msg.setPayloadLength(sizeof(payload));

    WRITE_HEADER(msg);

    // write payload
    boost::asio::write(socket, boost::asio::buffer(payload));

    EXPECT_EQ(boost::asio::read(socket, boost::asio::buffer(resp_header)), header_size);

    capnp::FlatArrayMessageReader resp_header_reader(
        kj::ArrayPtr<const capnp::word>((const capnp::word*)resp_header, header_size / word_size));
    auto response_root = resp_header_reader.getRoot<ObjectInstructionHeader>();

    ///////////////////////////////////////////////////////////////////

    this_msg.setInstruction(type::GET_OBJECT_CONTENT_BY_ID);
    WRITE_HEADER(msg);
    EXPECT_EQ(boost::asio::read(socket, boost::asio::buffer(resp_header)), header_size);

    // capnp::FlatArrayMessageReader resp_header_reader(
    //     kj::ArrayPtr<const capnp::word>((const capnp::word*)resp_header, header_size / word_size));
    // auto response_root = resp_header_reader.getRoot<ObjectInstructionHeader>();

    EXPECT_EQ(boost::asio::read(socket, boost::asio::buffer(payload)), sizeof(payload));
    EXPECT_EQ(response_root.getPayloadLength(), sizeof(payload));
    EXPECT_EQ(response_root.getInstruction(), type::GET_OBJECT_CONTENT_BY_ID);
    EXPECT_EQ(response_root.getObjectId().getField0(), 0);
    EXPECT_EQ(response_root.getObjectId().getField1(), 1);
    EXPECT_EQ(response_root.getObjectId().getField2(), 2);
    EXPECT_EQ(response_root.getObjectId().getField3(), 3);

    ///////////////////////////////////////////////////////////////////
    // NOTE: We have not set up name for this object
    this_msg.setInstruction(type::GET_OBJECT_NAME_BY_ID);
    WRITE_HEADER(msg);
    EXPECT_EQ(boost::asio::read(socket, boost::asio::buffer(resp_header)), header_size);

    response_root = resp_header_reader.getRoot<ObjectInstructionHeader>();
    EXPECT_EQ(response_root.getPayloadLength(), 0);

    // Don't read, this api block when no input
    // EXPECT_EQ(boost::asio::read(socket, boost::asio::buffer(payload)), 0);
    EXPECT_EQ(response_root.getInstruction(), type::GET_OBJECT_NAME_BY_ID);
    EXPECT_EQ(response_root.getObjectId().getField0(), 0);
    EXPECT_EQ(response_root.getObjectId().getField1(), 1);
    EXPECT_EQ(response_root.getObjectId().getField2(), 2);
    EXPECT_EQ(response_root.getObjectId().getField3(), 3);

    ///////////////////////////////////////////////////////////////////
    // NOTE: the object name is also set to the content of the payload, which is for conveninece
    this_msg.setInstruction(type::SET_OBJECT_NAME_BY_ID);
    WRITE_HEADER(msg);
    boost::asio::write(socket, boost::asio::buffer(payload));
    EXPECT_EQ(boost::asio::read(socket, boost::asio::buffer(resp_header)), header_size);

    response_root = resp_header_reader.getRoot<ObjectInstructionHeader>();
    EXPECT_EQ(response_root.getPayloadLength(), 0);

    EXPECT_EQ(response_root.getInstruction(), type::SET_OBJECT_NAME_BY_ID);
    EXPECT_EQ(response_root.getObjectId().getField0(), 0);
    EXPECT_EQ(response_root.getObjectId().getField1(), 1);
    EXPECT_EQ(response_root.getObjectId().getField2(), 2);
    EXPECT_EQ(response_root.getObjectId().getField3(), 3);

    // and close the connection now
    boost::system::error_code ec;
    auto res = socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    (void)res;
    socket.close();
}
