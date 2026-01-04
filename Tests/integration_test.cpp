// integration_test.cpp

#include "gtest/gtest.h"
#include "Server.h"
#include "Client.h"
#include "Utils.h"
#include <assert.h>


class TestClient : public Client 
{
public:
    TestClient(boost::asio::io_context& io, const std::string& host, uint16_t port, ESignalType signal_type, std::promise<bool>&& data_received_promise, std::promise<bool>&& snapshot_received_promise)
        : Client(io, host, port, signal_type)
        , m_data_received_promise(std::move(data_received_promise))
        , m_snapshot_received_promise(std::move(snapshot_received_promise))
    {}

    void SetInitEtalonData(MapSignal& map_init_etalon) { m_map_init_etalon = std::move(map_init_etalon); }
    void SetFinalEtalonData(MapSignal& map_final_etalon, uint32_t cnt_signal) { m_map_final_etalon = std::move(map_final_etalon); m_cnt_signal_final_state = cnt_signal; }

protected:
    void process_body(uint8_t type, const std::vector<uint8_t>& body) override
    {
        if (type == 0x02)
        {
            std::lock_guard<std::mutex> lock(m_mtx_signal);

            size_t pos = 0;
            while (pos + 13 <= body.size())
            {
                // id
                uint32_t id;
                std::memcpy(&id, body.data() + pos, 4);
                id = ntohl(id);
                pos += 4;

                // type
                uint8_t type = body[pos++];

                // value
                uint64_t ubits;
                std::memcpy(&ubits, body.data() + pos, 8);
                ubits = net_to_host_u64(ubits);
                pos += 8;

                double val;
                std::memcpy(&val, &ubits, sizeof(val));

                Signal s(id, static_cast<ESignalType>(type), val);

                // check seconds msgs
                if (m_cnt_packet > 1 && m_map_signal.find(id) == m_map_signal.end())
                {
                    // signal was received that was not in the full first packet - it is error
                    m_data_received_promise.set_value(false);
                    m_data_rcvd_promise_set = true;
                    Stop();
                    break;
                }

                m_map_signal[id] = s;

                m_cnt_signal++;

            }

            if (m_cnt_packet == 1)
            {
                // check first msg:
                // full first packet recieved, compare it with m_map_init_etalon
                if (m_map_signal != m_map_init_etalon)
                {
                    // data error
                    m_snapshot_received_promise.set_value(false);
                    Stop();
                }
                else
                {
                    // ok
                    m_snapshot_received_promise.set_value(true);
                }
            }
            else //if (m_cnt_packet > 1) 
            {
                // compare m_map_signal & m_map_final_etalon and also compare count rcvd signal 
                if (m_map_signal == m_map_final_etalon && 
                    m_cnt_signal_final_state == m_cnt_signal)
                {
                    if (!m_data_rcvd_promise_set)
                    {
                        //  ok, set final flag
                        m_data_received_promise.set_value(true);
                        m_data_rcvd_promise_set = true;
                    }
                    Stop();
                }
            }
        }

    }

private: 
    // final data check
    std::promise<bool> m_data_received_promise;
    bool m_data_rcvd_promise_set = false;

    // first full packet check
    std::promise<bool> m_snapshot_received_promise;

    // etalon data
    MapSignal m_map_init_etalon;
    MapSignal m_map_final_etalon;
    uint32_t m_cnt_signal_final_state;

    //count rcvd signals
    uint32_t m_cnt_signal{ 0 };
};


void add_signal_changes(int cnt_circle, VecSignal& signals, std::map<uint32_t, Signal>& map_state)
{
    std::mt19937 rng((unsigned)std::chrono::system_clock::now().time_since_epoch().count());

    // id
    std::uniform_int_distribution<int> ids(1, signals.size());

    // value
    std::uniform_int_distribution<int> discret_val(0, 1);
    std::uniform_real_distribution<double> delta(-0.5, 0.5);

    // init map
    for (auto s : signals)
    {
        map_state[s.id] = s;
    }

    VecSignal signal_changes;

    for (int i = 0; i < cnt_circle * signals.size(); i++)
    {
        int id = ids(rng);
        auto it = map_state.find(id);
        if (it == map_state.end())
        {
            continue;
        }

        Signal& s = it->second;
        s.value = (s.type == ESignalType::discret) ? (discret_val(rng)) : it->second.value + delta(rng);
        s.ts = std::chrono::steady_clock::now(); // now in Signal::operator==() compare time is disable

        signal_changes.push_back(s);
    }

    signals.insert(signals.end(), signal_changes.begin(), signal_changes.end());
}

VecSignal make_wrong_time_signals(const VecSignal& signals)
{
    VecSignal wrong_signals;

    for (auto s : signals)
    {
        s.value = s.type == ESignalType::discret ? 1 : 0.5;

        std::chrono::seconds back_off(600);
        s.ts -= back_off;

        wrong_signals.push_back(s);
    }

    return wrong_signals;
}

TEST(IntegrationTest, DataLogicTest)
{
    try
    {

        boost::asio::io_context io;
        auto work_guard_server = boost::asio::make_work_guard(io);

        // server
        Server server(io, 5000);

        server.EnableDataEmulation(false);         // disable data producing
        server.EnableShowLogMsg(false);

        std::thread io_thread_srv([&io]() { io.run(); });

        // prepare test data
        VecSignal test_signals =
        {
            {1, ESignalType::discret, 0},
            {2, ESignalType::analog, 10.0},
            {3, ESignalType::discret, 1},
            {4, ESignalType::analog, 12.5},
        };

        server.SetSignals(test_signals);
        server.EnableShowLogMsg(false);

        server.Start();


        // data for waiting for a result from an client
        std::promise<bool> data_received_promise;
        std::future<bool> data_received_future = data_received_promise.get_future();

        std::promise<bool> snapshot_ready_promise;
        std::future<bool> snapshot_ready_future = snapshot_ready_promise.get_future();

        // test client 
        boost::asio::io_context io_client;
        TestClient client(io_client, "127.0.0.1", 5000, (ESignalType::discret | ESignalType::analog), std::move(data_received_promise), std::move(snapshot_ready_promise));

        client.EnableShowLogMsg(false);


        //////////////////////////////////////////////////////////////////////////



        // Stage 1: Waiting for first full packet

        // etalon data for first msg
        MapSignal map_init_etalon;
        for (auto& s : test_signals)
        {
            map_init_etalon[s.id] = s;
        }

        client.SetInitEtalonData(map_init_etalon);

        auto work_guard_client = boost::asio::make_work_guard(io_client);
        std::thread io_thread_client([&io_client]() { io_client.run(); });

        client.Start();


        // wait until the client processes the first full packet
        std::future_status status_snapshot = snapshot_ready_future.wait_for(std::chrono::seconds(5));
        ASSERT_EQ(std::future_status::ready, status_snapshot) << "Client failed to receive initial snapshot in time.";



        //////////////////////////////////////////////////////////////////////////

        // Stage 2: Sending Updates and check final data

        // etalon data for end test
        MapSignal map_final_etalon;
 
        // the number of signals the client should receive
        int cnt = test_signals.size(); // count for first msg

        // add new signals to test_signals
        add_signal_changes(10, test_signals, map_final_etalon);
        cnt += test_signals.size(); // add number of changed signals

        client.SetFinalEtalonData(map_final_etalon, cnt);

        ////
        // add bad signals in the middle. these signals (with old time) should not reach the client
        auto wrong_signals = make_wrong_time_signals(test_signals);
        test_signals.insert(test_signals.begin() + test_signals.size() / 2, wrong_signals.begin(), wrong_signals.end());
        ////


        // send all it to server
        int i = 0;
        for (auto& s : test_signals)
        {
            server.PushSignal(s);

            if (i++ % 20 == 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Just in case: this is so that the all signals are not sent to clinet in one packet
        }

        // waiting for the final test to be completed
        std::future_status status_final = data_received_future.wait_for(std::chrono::seconds(5));

        // check final_etalon
        ASSERT_EQ(std::future_status::ready, status_final) << "Client failed to receive updates in time.";
        ASSERT_TRUE(data_received_future.get()) << "Final validation failed.";


        //////////////////////////////////////////////////////////////////////////

        // cleanup

        server.Stop();
        client.Stop();

        work_guard_server.reset();
        work_guard_client.reset();

        io_thread_srv.join();
        io_thread_client.join();

    }
    catch (const boost::system::system_error& e) 
    {
        FAIL() << "FATAL BOOST ERROR: " << e.what() << " (Code: " << e.code() << ")";
    }
    catch (const std::exception& e) 
    {
        FAIL() << "FATAL C++ EXCEPTION: " << e.what();
    }
    catch (...) 
    {
        FAIL() << "FATAL UNKNOWN EXCEPTION CAUGHT.";
    }


}

