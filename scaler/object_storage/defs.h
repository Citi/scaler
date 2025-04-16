#pragma once

#include <array>
#include <boost/asio/ip/tcp.hpp>
#include <memory>
#include <vector>

#include "protocol/object_instruction_header.capnp.h"

namespace scaler {
namespace object_storage {

using object_id_t = std::array<uint64_t, 4>;
// using object_name_t   = std::vector<unsigned char>;
using object_t        = std::vector<unsigned char>;
using shared_object_t = std::shared_ptr<object_t>;
using payload_t       = std::vector<unsigned char>;

struct ObjectRequestHeader {
    object_id_t object_id;
    uint64_t payload_length;
    ::ObjectRequestHeader::ObjectRequestType req_type;
    ObjectRequestHeader(): object_id {}, payload_length {}, req_type {} {}
};

struct ObjectResponseHeader {
    object_id_t object_id;
    uint64_t payload_length;
    ::ObjectResponseHeader::ObjectResponseType resp_type;
    ObjectResponseHeader(): object_id {}, payload_length {}, resp_type {} {}
};

// TODO: move only
struct meta {
    boost::asio::ip::tcp::socket socket;
    ObjectRequestHeader request_header;
    ObjectResponseHeader response_header;
};

struct object_with_meta {
    shared_object_t object;
    std::optional<meta> meta_info;
};

};  // namespace object_storage
};  // namespace scaler
