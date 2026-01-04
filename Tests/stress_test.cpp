// stress_test.cpp

#include <gtest/gtest.h>
#include <boost/asio.hpp>
#include <thread>
#include <atomic>
#include <vector>
#include <memory>
#include <future>
#include <chrono>
#include "Server.h"
#include "Client.h"


const int NUM_CLIENTS = 100; 
const int NUM_CYCLES = 25*1000; // one circle: push 4 singnals. total - 100.000 signals


class StressClient : public Client
{
public:
    StressClient(boost::asio::io_context& io, const std::string& host, uint16_t port, std::atomic<int>& ready_clients_count, std::atomic<int>& finished_clients_count, int num_signal_wait)
        : Client(io, host, port, (ESignalType::discret | ESignalType::analog))
        , m_ready_clients_count(ready_clients_count)
        , m_finished_clients_count(finished_clients_count)
        , m_num_signal_wait(num_signal_wait)
    {
    }

    StressClient(const StressClient&) = delete;
    StressClient& operator=(const StressClient&) = delete;


protected:
    void process_body(uint8_t type, const std::vector<uint8_t>& body) override
    {
        if (type == 0x02)
        {
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

                ////std::cout << "Update: id=" << id << " type=" << int(type) << " val=" << val << "\n";

                m_cnt_signal++;

            }

            if (m_cnt_packet == 1 && m_cnt_signal != 0)
            {
                // first data rcvd
                m_ready_clients_count.fetch_add(1);
            }

            if(m_cnt_signal == m_num_signal_wait)
            {
                // ok. set final flag
                m_finished_clients_count.fetch_add(1);

                Stop();
            }

        }

    }

private:
    std::atomic<int>& m_finished_clients_count;
    std::atomic<int>& m_ready_clients_count;

    int m_num_signal_wait;

    uint32_t m_cnt_signal{ 0 };
};



TEST(StressTest, MultipleClientsLoadTest)
{
    try
    {

        // init server
        boost::asio::io_context io_server;
        auto work_guard_server = boost::asio::make_work_guard(io_server);
        Server server(io_server, 5000);

        std::thread io_thread_server([&io_server]() { io_server.run(); });

        server.EnableDataEmulation(false);
        server.EnableShowLogMsg(false);

        // test data
        VecSignal test_signals =
        {
            {1, ESignalType::discret, 0},
            {2, ESignalType::analog, 10.0},
            {3, ESignalType::discret, 1 },
            {4, ESignalType::analog, -12.0}
        };
        server.SetSignals(test_signals);


        server.Start();


        // counter - how many clients have received snapshots and are ready for updates
        std::atomic<int> ready_clients_count{ 0 };

        // waiting for all clients to complete
        std::atomic<int> finished_clients_count{ 0 };


        // Clients

        std::vector<boost::asio::io_context> client_contexts(NUM_CLIENTS);

        std::vector<std::unique_ptr<StressClient>> clients;

        std::vector<std::thread> client_threads;

        using client_work_guard_t = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;
        std::vector<client_work_guard_t> client_work_guards;

        const int num_signal_wait = (NUM_CYCLES + 1) * test_signals.size(); // +1: first full packet with signals - from { server.SetSignals(test_signals) }

        // create & run clients
        int cnt = 0;
        for (int i = 0; i < NUM_CLIENTS; ++i)
        {
            boost::asio::io_context& io_cli = client_contexts[i];

            client_work_guards.emplace_back(boost::asio::make_work_guard(io_cli));

            // client
            clients.emplace_back(std::make_unique<StressClient>(
                io_cli,
                "127.0.0.1",
                5000,
                std::ref(ready_clients_count),
                std::ref(finished_clients_count),
                num_signal_wait
            ));

            client_threads.emplace_back([&ctx = client_contexts[i]]() {
                ctx.run();
                });

            // start client
            clients.back()->EnableShowLogMsg(false);
            clients.back()->Start();
            cnt++;
        }

        //////////////////////////////////////////////////////////////////////////

        // wait until all clients connect - then they are guaranteed to receive the same number of signals ( via next server.PushSignal() )
        auto start_time = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::seconds(25);
        
        while (ready_clients_count.load() < NUM_CLIENTS)
        {
            if (std::chrono::steady_clock::now() - start_time > timeout)
            {
                FAIL() << "Timeout: Not all clients received snapshot. Ready: "
                    << ready_clients_count.load() << "/" << NUM_CLIENTS;

                goto cleanup;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        ASSERT_EQ(NUM_CLIENTS, ready_clients_count.load()) << "Clients readiness synchronization error.";

        //////////////////////////////////////////////////////////////////////////
        


        // run stress circle (PushSignal)
        for (int i = 0; i < NUM_CYCLES; ++i)
        {
            for (const auto& signal : test_signals)
            {
                server.PushSignal(signal);
            }

            if (i % 5000 == 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100)); //this is so that the all signals are not sent to clinet in one packet
            }
        }

        // waiting for final complete
        while (finished_clients_count.load() < NUM_CLIENTS)
        {
            if (std::chrono::steady_clock::now() - start_time > timeout)
            {
                FAIL() << "Total timeout exceeded waiting for all clients. Finished: "
                    << finished_clients_count.load() << "/" << NUM_CLIENTS;
                goto cleanup;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        ASSERT_EQ(NUM_CLIENTS, finished_clients_count.load()) << "error: not all clients completed their work.";



    cleanup:

        for (auto& client : clients) 
        {
            client->Stop();
            client.reset();
        }
        server.Stop();

        work_guard_server.reset();

        for (auto& cwg : client_work_guards)
        {
            cwg.reset();
        }

        io_thread_server.join();

        for (auto& thread : client_threads) 
        {
            if (thread.joinable()) 
            {
                thread.join();
            }
        }
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