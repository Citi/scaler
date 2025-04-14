

// echo_server.cpp
// ~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2025 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <capnp/message.h>
#include <capnp/serialize.h>

#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/write.hpp>
#include <cstdio>
#include <iostream>

#include "object_instruction_header.capnp.h"

using boost::asio::ip::tcp;

int main() {
    boost::asio::io_context io_context;

    // we need a socket and a resolver
    tcp::socket socket(io_context);
    tcp::resolver resolver(io_context);

    // now we can use connect(..)
    boost::asio::connect(socket, resolver.resolve("127.0.0.1", "55555"));

    capnp::MallocMessageBuilder msg;
    auto this_msg                     = msg.initRoot<ObjectInstructionHeader>();
    std::array<uint64_t, 4> object_id = {1, 2, 3, 4};

    auto obj_id_builder = this_msg.initObjectId();
    obj_id_builder.setField0(object_id[0]);
    obj_id_builder.setField1(object_id[1]);
    obj_id_builder.setField2(object_id[2]);
    obj_id_builder.setField3(object_id[3]);

    this_msg.setInstruction(::ObjectInstructionHeader::ObjectInstructionType::SET_OBJECT_CONTENT_BY_ID);

    this_msg.setPayloadLength(strlen("Hello, world!"));

    auto flat_array = capnp::messageToFlatArray(msg);
    auto result =
        boost::asio::write(socket, boost::asio::buffer(flat_array.asBytes().begin(), flat_array.asBytes().size()));

    boost::asio::write(socket, boost::asio::buffer("Hello, world!", strlen("Hello, world!")));

    char buf[72];
    auto n = boost::asio::read(socket, boost::asio::buffer(buf));
    assert(n == 72);

    // and close the connection now
    boost::system::error_code ec;
    socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    socket.close();

    return 0;
}
