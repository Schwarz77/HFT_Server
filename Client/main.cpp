#include "Client.h"
#include <iostream>


int main(int argc, char* argv[])
{
    try
    {
        std::string host = "127.0.0.1";
        uint16_t port = 6000;

        std::string symbol("BTCUSDT");
        double treshold = 100'000;
        bool ext_vwap = false;

        EProtocolDataType reqType = EProtocolDataType::Whale | EProtocolDataType::VWAP;

        if (argc >= 2)
            host = argv[1];

        if (argc >= 3)
            port = static_cast<uint16_t>(std::atoi(argv[2]));

        if (argc >= 4)
            reqType = static_cast<EProtocolDataType>(std::atoi(argv[3]));

        if (argc >= 5)
            symbol = argv[4];

        if (argc >= 6)
            treshold = std::atof(argv[5]);

        if (argc >= 7)
            ext_vwap = static_cast<bool>(std::atoi(argv[6]));

        boost::asio::io_context io;

        Client client(io, host, port, reqType, symbol);

        client.SetWhaleTreshold(treshold);
        client.SetExtVwap(ext_vwap);


        client.Start();


        io.run();

    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}