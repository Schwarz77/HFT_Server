#pragma once


#include <boost/system/error_code.hpp>
#include <cstring>
#include <cmath>


inline bool double_equals(double a, double b, double epsilon = std::numeric_limits<double>::epsilon())
{
    return std::fabs(a - b) < epsilon;
}


std::string win32_message_english(unsigned long /*DWORD*/ code);
void write_error(const std::string& text, const boost::system::error_code& ec);



static inline uint16_t host_to_net_u16(uint16_t x)
{
#if defined(_WIN32)
    return _byteswap_ushort(x);
#elif __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return __builtin_bswap16(x);
#else
    return x;
#endif
}

static inline uint32_t net_to_host_u16(uint16_t x)
{
    return host_to_net_u16(x);
}


static inline uint32_t host_to_net_u32(uint32_t x)
{
#if defined(_WIN32)
    return _byteswap_ulong(x);
#elif __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return __builtin_bswap32(x);
#else
    return x;
#endif
}

static inline uint32_t net_to_host_u32(uint32_t x)
{
    return host_to_net_u32(x);
}

static inline uint64_t net_to_host_u64(uint64_t x)
{
#if defined(_WIN32)
    return _byteswap_uint64(x);
#elif __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return __builtin_bswap64(x);
#else
    return x;
#endif
}

static inline uint64_t host_to_net_u64(uint64_t x)
{
    return net_to_host_u64(x);
}

