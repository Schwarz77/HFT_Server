// utility_test.cpp

#include "gtest/gtest.h"
#include "Utils.h"


TEST(UtilityTest, HostToNet16Conversion) 
{
    // value: 0x12345678 (host-order)
    uint16_t host_value = 0x1234;
    
    // In network (big-endian) order this should be 0x3412
    uint16_t expected_net_value = 0x3412; 

    // check that the function correctly changes the byte order
    ASSERT_EQ(expected_net_value, host_to_net_u16(host_value));
    
    // Checking that the inverse transformation works
    ASSERT_EQ(host_value, net_to_host_u16(expected_net_value));
}

TEST(UtilityTest, HostToNet32Conversion)
{
    // value: 0x12345678 (host-order)
    uint32_t host_value = 0x12345678;

    // In network (big-endian) order this should be 0x78563412
    uint32_t expected_net_value = 0x78563412;

    // check that the function correctly changes the byte order
    ASSERT_EQ(expected_net_value, host_to_net_u32(host_value));

    // Checking that the inverse transformation works
    ASSERT_EQ(host_value, net_to_host_u32(expected_net_value));
}

TEST(UtilityTest, HostToNet64Conversion)
{
    // value: 0x12345678 (host-order)
    uint64_t host_value = 0x1234567844332211;

    // In network (big-endian) order this should be 0x1122334478563412
    uint64_t expected_net_value = 0x1122334478563412;

    // check that the function correctly changes the byte order
    ASSERT_EQ(expected_net_value, host_to_net_u64(host_value));

    // Checking that the inverse transformation works
    ASSERT_EQ(host_value, net_to_host_u64(expected_net_value));
}