#pragma once

#include <boost/asio.hpp>
#include <string>
#include <vector>
#include <cstdint>
#include "Protocol.h"



class Client
{
public:
    Client(boost::asio::io_context& io, const std::string& host, uint16_t port, ESignalType signal_type);
    virtual ~Client();

    // disable copying
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    void Start();
    void Stop();

    void EnableShowLogMsg(bool is_enable) { m_show_log_msg = is_enable; }
    bool IsShowLogMsg() { return m_show_log_msg; }

    MapSignal GeSignals();
    uint64_t GetPacketCount() { return m_cnt_packet; }

private:
    void connect();
    void send_subscribe();
    void start_read_header();
    void start_read_body(uint32_t len, uint8_t data_type);
    virtual void process_body(uint8_t type, const std::vector<uint8_t>& body);
    void schedule_reconnect();
    void clear_data();

protected:
    boost::asio::io_context& m_io;

    boost::asio::ip::tcp::socket m_socket;
    boost::asio::ip::tcp::resolver m_resolver;
    boost::asio::steady_timer m_reconnect_timer;

    std::string m_host;
    uint16_t m_port;
    ESignalType m_signal_type;

    // inbound buffers/state
    SSignalProtocolHeader m_header;
    std::vector<uint8_t> m_body;

    std::atomic<uint64_t> m_cnt_packet{0};

    std::mutex m_mtx_signal;
    MapSignal m_map_signal;

    std::atomic<bool> m_show_log_msg{ true };
};

