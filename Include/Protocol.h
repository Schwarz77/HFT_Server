#pragma once

#include <cstdint>
#include <type_traits>
#include <chrono>
#include <map>
#include "Utils.h"


// Header layout (9 bytes, network byte order / big-endian):
// uint16_t signature (0xAA55)
// uint8_t  version (1)
// uint8_t  dataType (1=Subscribe (to server), 2=Data (to client), 3=Alive (to client))
// uint8_t  msg_num (order msg number)
// uint32_t len (payload length)


#pragma pack(push,1)
struct SProtocolHeader
{
    uint16_t signature;
    uint8_t  version;
    uint8_t  data_type;
    uint8_t  msg_num;
    uint32_t len;
};
#pragma pack(pop)
static_assert(sizeof(SProtocolHeader) == 9, "Header must be 9 bytes");

const uint16_t PROTOCOL_HEADER_SIGNATURE = 0xAA55;


// Signals

enum class EProtocolDataType : uint8_t
{
    unknown = 0,
    Whale = 1 << 0,
    VWAP = 1 << 1,
};

inline EProtocolDataType operator|(EProtocolDataType lhs, EProtocolDataType rhs)
{
    using T = std::underlying_type_t<EProtocolDataType>;
    return static_cast<EProtocolDataType>(static_cast<T>(lhs) | static_cast<T>(rhs));
}
