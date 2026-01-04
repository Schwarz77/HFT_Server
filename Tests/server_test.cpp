// server_test.cpp

#include "gtest/gtest.h"
#include "Server.h"


class TestServer : public Server 
{
public:
    TestServer(boost::asio::io_context& io) 
        : Server(io, 0) 
    {}

    void FillState(const VecSignal& signals) 
    {
        std::lock_guard<std::mutex> lk(m_mtx_state);
        for (const auto& s : signals) 
        {
            m_state[s.id] = s;
        }
    }
};

TEST(ServerBasic, StartStop) 
{
    boost::asio::io_context io;
    Server server(io, 0);
    server.EnableShowLogMsg(false);

    EXPECT_NO_THROW(server.Start());
    EXPECT_NO_THROW(server.Stop());
}

TEST(ServerTest, SnapshotFiltering) 
{
    boost::asio::io_context io;
    TestServer server(io);

    server.EnableShowLogMsg(false);
    server.EnableDataEmulation(false);

    // prepare test data
    VecSignal test_signals = 
    {
        {1, ESignalType::discret, 0.0},
        {2, ESignalType::analog, 10.0},
        {3, ESignalType::discret, 0.0},
    };
    server.FillState(test_signals);

    // require discret (0x01)
    uint8_t discret_mask = (uint8_t)ESignalType::discret;
    auto snapshot_discret = server.GetSnapshot(discret_mask);
    
    // wait 2 discret signals (ID 1, 3)
    ASSERT_EQ(2, snapshot_discret.size());
    ASSERT_EQ(ESignalType::discret, snapshot_discret[0].type); 

    // require analog (0x02)
    uint8_t analog_mask = (uint8_t)ESignalType::analog;
    auto snapshot_analog = server.GetSnapshot(analog_mask);
    
    // wait 1 discret signal (ID 2)
    ASSERT_EQ(1, snapshot_analog.size());
    ASSERT_EQ(ESignalType::analog, snapshot_analog[0].type); 
}