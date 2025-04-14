#pragma once

#include <capnp/common.h>

#include <cstddef>

static constexpr size_t CAPNP_HEADER_SIZE     = 72;
static constexpr size_t CAPNP_WORD_SIZE       = sizeof(capnp::word);
static constexpr size_t MEMORY_LIMIT_IN_BYTES = 6uz << 40;  // 6 TB
