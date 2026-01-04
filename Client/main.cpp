#include "Client.h"
#include <iostream>


int main(int argc, char* argv[])
{
    try
    {
        std::string host = "127.0.0.1";
        uint16_t port = 6000;

        ESignalType reqType = ESignalType::discret | ESignalType::analog;

        if (argc >= 2)
            host = argv[1];

        if (argc >= 3)
            port = static_cast<uint16_t>(std::atoi(argv[2]));

        if (argc >= 4)
            reqType = static_cast<ESignalType>(std::atoi(argv[3]));


        boost::asio::io_context io;

        Client client(io, host, port, reqType);

        client.Start();

        io.run();
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}