// perf_test.cpp

#include <gtest/gtest.h>
#include <boost/asio.hpp>
#include <chrono>
#include "Server.h"

TEST(Perf, ServerThroughput) 
{
    using namespace std::chrono;

    boost::asio::io_context io;
    Server server(io, 0);

    server.EnableShowLogMsg(false);
    server.EnableDataEmulation(false);

    std::thread th([&]() { io.run(); });

    const int N = 5000;
    auto t0 = high_resolution_clock::now();

    for (int i = 0; i < N; i++) 
    {
        Signal s{ (uint32_t)i, ESignalType::analog, double(i) };
        server.PushSignal(s);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    auto t1 = high_resolution_clock::now();
    auto ms = duration_cast<milliseconds>(t1 - t0).count();

    std::cout << "\nPerf test: " << N << " updates in " << ms << " ms\n";

    EXPECT_LT(ms, 200);

    server.Stop();
    th.join();
}