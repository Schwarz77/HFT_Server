#include <boost/asio.hpp>
#include <iostream>
#include "Server.h"

namespace io = boost::asio;

using time_point = std::chrono::steady_clock::time_point;
using steady_clock = std::chrono::steady_clock;


static Server* g_pServer = NULL;

void signal_handler(int s) 
{
    if (g_pServer)
        g_pServer->Stop();
}

int main(int argc, char* argv[])
{
    try
    {
        io::io_context io;

        uint16_t port = 6000;
        bool data_emulation = false;
        bool ext_vwap = false;

        if (argc >= 2)
            port = static_cast<uint16_t>(std::atoi(argv[1]));

        if (argc >= 3)
            data_emulation = static_cast<bool>(std::atoi(argv[2]));

        if (argc >= 4)
            ext_vwap = static_cast<bool>(std::atoi(argv[3]));


        Server server(io, 6000);
        g_pServer = &server;

        signal(SIGINT, signal_handler);

        server.EnableDataEmulation(data_emulation);
        server.SetExtCalcVWAP(ext_vwap);
        server.EnableShowLogMsg(true);

        server.Start();

        io.run();

    }
    catch (std::exception& ex)
    {
        std::cerr << "\n" << ex.what() << "\n";
    }

    return 0;
}

