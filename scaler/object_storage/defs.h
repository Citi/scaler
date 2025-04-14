#pragma once

#include <array>
#include <memory>
#include <vector>

#include "object_instruction_header.capnp.h"

using object_id_t     = std::array<uint64_t, 4>;
using object_name_t   = std::vector<unsigned char>;
using object_t        = std::vector<unsigned char>;
using shared_object_t = std::shared_ptr<object_t>;
using payload_t       = std::vector<unsigned char>;

struct object_with_meta {
    shared_object_t object;
    std::optional<object_name_t> name;
};

struct object_header {
    object_id_t object_id;
    uint64_t payload_length;
    ObjectInstructionHeader::ObjectInstructionType ins_type;

    object_header(): object_id {}, payload_length {}, ins_type {} {}
};
