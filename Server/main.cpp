#include <boost/asio.hpp>
#include <iostream>
#include "Server.h"

namespace io = boost::asio;

using time_point = std::chrono::steady_clock::time_point;
using steady_clock = std::chrono::steady_clock;


// #define TEST_SERVER_API

#ifdef TEST_SERVER_API
// test server API
void thread_set_signals(Server& server, size_t signal_count, bool& isStop, std::mutex& mtx, std::condition_variable& cv);
void thread_update_signals(Server& server, size_t signal_count, bool& isStop, std::mutex& mtx, std::condition_variable& cv);
#endif


static Server* g_pServer = NULL;

void signal_handler(int s) 
{
    if (g_pServer)
        g_pServer->Stop();
}

int main() 
{
    try
    {
        io::io_context io;

        Server server(io, 6000);
        g_pServer = &server;

        signal(SIGINT, signal_handler);


        server.EnableDataEmulation(true);
        server.EnableShowLogMsg(true);

        //VecSignal signals = 
        //{   Signal{ 1, ESignalType::discret } ,
        //    Signal{ 2, ESignalType::discret },
        //    Signal{ 3, ESignalType::analog },
        //    Signal{ 4, ESignalType::analog },
        //};

        //server.SetSignals(signals);

        server.Start();


#ifdef TEST_SERVER_API
        /// test server API
        bool isStop = false;
        std::mutex mtx;
        std::condition_variable cv;
        std::thread tss(thread_set_signals, std::ref(server), signals.size(), std::ref(isStop), std::ref(mtx), std::ref(cv));
        std::thread tsu(thread_update_signals, std::ref(server), signals.size(), std::ref(isStop), std::ref(mtx), std::ref(cv));
#endif


        io.run();


#ifdef TEST_SERVER_API
        isStop = true;
        tss.join();
        tsu.join();
#endif

    }
    catch (std::exception& ex)
    {
        std::cerr << "\n" << ex.what() << "\n";
    }

    return 0;
}

#ifdef TEST_SERVER_API
void thread_set_signals(Server& server, size_t signal_count, bool& isStop, std::mutex& mtx, std::condition_variable& cv)
{
    while (true)
    {
        {
            std::unique_lock<std::mutex> lock(mtx);

            if (cv.wait_for(lock, std::chrono::seconds(5), [&isStop] { return isStop; }))
            {
                break;
            }
        }

        VecSignal signals;
        for (int i = 0; i < signal_count; i += 2)
        {
            signals.push_back(Signal(i, ESignalType::discret));
            signals.push_back(Signal(i + 1, ESignalType::analog));
        }

        server.SetSignals(signals);
    }
}

void thread_update_signals(Server& server, size_t signal_count, bool& isStop, std::mutex& mtx, std::condition_variable& cv)
{
    std::mt19937 rng((unsigned)std::chrono::system_clock::now().time_since_epoch().count());

    // id
    std::uniform_int_distribution<int> ids(1, signal_count);

    // value
    std::uniform_int_distribution<int> discret_val(0, 1);
    std::uniform_real_distribution<double> delta(-0.5, 0.5);


    while (true)
    {
        {
            std::unique_lock<std::mutex> lock(mtx);

            if (cv.wait_for(lock, std::chrono::milliseconds(100), [&isStop] { return isStop; }))
            {
                break;
            }
        }

        int id = ids(rng);

        Signal s;

        if (server.GetSignal(id, s))
        {
            s.value = (s.type == ESignalType::discret) ? (discret_val(rng)) : s.value + delta(rng);
            s.ts = steady_clock::now();

            server.PushSignal(s);
        }
    }
}
#endif