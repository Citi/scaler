#pragma once

// C
#include <cstddef>
#include <cstdint>
#include <cstring>

// C++
#include <string>
#include <iostream>
#include <source_location>

// System
#include <sys/timerfd.h>

// 5ca1ab1e = scalable
static uint8_t MAGIC[4] = {0x5c, 0xa1, 0xab, 0x1e};

// Python callback
void future_set_result(void *future, void *data);

// this is an unrecoverable error that exits the program
// prints a message plus the source location
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

struct Bytes
{
    uint8_t *data;
    size_t len;

    bool operator==(const Bytes &other) const
    {
        if (len != other.len)
            return false;

        return std::memcmp(data, other.data, len) == 0;
    }

    // same as empty()
    bool operator!() const
    {
        return len == 0 || data == nullptr;
    }

    bool empty() const
    {
        return len == 0 || data == nullptr;
    }

    // debugging utility
    std::string as_string() const
    {
        if (empty())
        {
            return "[EMPTY]";
        }

        return std::string((char *)data, len);
    }
};

struct Message
{
    // the address the message was received from, or to send to
    //
    // for received messages, the address data is
    // owned by the peer it was received from
    Bytes address;

    // the payload of the message
    //
    // for received messages, the payload data is owned
    // and must be freed when the message is destroyed
    Bytes payload;
};

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

typedef uint64_t timerfd_t;

// timerfd analogue of eventfd_read()
int timerfd_read(int fd, timerfd_t *value)
{
    auto n = read(fd, value, sizeof(timerfd_t));

    if (n != sizeof(timerfd_t))
        return -1;

    return 0;
}

// read a timerfd value and discard it
// return value is the same as timerfd_read()
int timerfd_read2(int fd) {
    timerfd_t value;
    return timerfd_read(fd, &value);
}
