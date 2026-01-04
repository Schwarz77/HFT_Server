// session_test.cpp

#include <gtest/gtest.h>
#include <boost/asio.hpp>
#include "Session.h"
#include "Server.h"


TEST(SessionBasic, Construct) 
{
	boost::asio::io_context io;
	Server server(io, 0);
	boost::asio::ip::tcp::socket sock(io);

	auto s = std::make_shared<Session>(std::move(sock), server);
	EXPECT_TRUE(s != nullptr);
}