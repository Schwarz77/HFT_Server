// client.cpp


#include "Client.h"
#include <boost/asio.hpp>
#include <iostream>
#include <vector>
#include <cstring>
#include <Utils.h>

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;


Client::Client(asio::io_context& io, const std::string& host, uint16_t port, EProtocolDataType signal_type, std::string& coin_symbol)
    : m_io(io),
    m_socket(io),
    m_resolver(io),
    m_reconnect_timer(io),
    m_host(host),
    m_port(port),
    m_data_type(signal_type),
    m_coin_symbol(coin_symbol)
{
}

Client::~Client()
{
    Stop();
}

void Client::Start()
{
    m_reconnect_timer.cancel();
    connect();

    if(m_show_log_msg)
        std::cout << "Client started [" << m_coin_symbol << "]" << std::endl;
}

void Client::Stop()
{
    error_code ec;

    m_reconnect_timer.cancel();

    if (m_socket.is_open())
    {
        m_socket.cancel(ec);
        m_socket.shutdown(tcp::socket::shutdown_both, ec);
        m_socket.close(ec);
    }
}

void Client::connect()
{
    m_resolver.async_resolve(m_host, std::to_string(m_port),
        [this](const error_code& ec, tcp::resolver::results_type endpoints)
        {
            if (ec == asio::error::operation_aborted)
            {
                return;
            }

            if (ec)
            {
                write_error("Resolve failed", ec);
                schedule_reconnect();
                return;
            }

            asio::async_connect(m_socket, endpoints,
                [this](const error_code& ec, const tcp::endpoint& ep)
                {
                    if (ec == asio::error::operation_aborted)
                    {
                        return;
                    }

                    if (ec)
                    {
                        write_error("Connect failed", ec);
                        schedule_reconnect();
                        return;
                    }

                    if (m_show_log_msg)
                        std::cout << "Connected to server\n";

                    clear_data();

                    send_subscribe();
                });
        });
}

void Client::send_subscribe()
{
    // Build subscribe payload
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(m_data_type));


    //symbol name
    payload.push_back(m_coin_symbol.length());
    payload.insert(payload.end(), (uint8_t*)m_coin_symbol.data(), (uint8_t*)m_coin_symbol.data() + m_coin_symbol.length());


    // treshold
    uint64_t treshold;
    static_assert(sizeof(m_treshold) == sizeof(m_treshold), "double size mismatch");
    std::memcpy(&treshold, &m_treshold, sizeof(treshold));
    treshold = host_to_net_u64(treshold);
    payload.insert(payload.end(), (uint8_t*)&treshold, (uint8_t*)&treshold + 8);



    // Header
    SProtocolHeader hdr;
    hdr.signature = host_to_net_u16(PROTOCOL_HEADER_SIGNATURE);
    hdr.version = 1;
    hdr.data_type = 0x01; // subscribe
    hdr.msg_num = 0;
    hdr.len = host_to_net_u32(static_cast<uint32_t>(payload.size()));  // data length 

    std::vector<uint8_t> frame(sizeof(hdr) + payload.size());
    std::memcpy(frame.data(), &hdr, sizeof(hdr));
    if (!payload.empty())
    {
        std::memcpy(frame.data() + sizeof(hdr), payload.data(), payload.size());
    }

    auto buf = std::make_shared<std::vector<uint8_t>>(std::move(frame));

    asio::async_write(m_socket, asio::buffer(*buf),
        [this, buf](const error_code& ec, std::size_t /*bytes_transferred*/)
        {
            if (ec == asio::error::operation_aborted)
            {
                return;
            }

            if (ec)
            {
                write_error("Write subscribe failed", ec);
                schedule_reconnect();
                return;
            }

            start_read_header();
        });
}

void Client::start_read_header()
{
    if (!m_socket.is_open())
    {
        return;
    }

    asio::async_read(m_socket, asio::buffer(&m_header, sizeof(m_header)),
        [this](const error_code& ec, std::size_t /*n*/)
        {
            if (ec)
            {
                if (ec == asio::error::eof || ec == asio::error::connection_reset)
                {
                    std::cout << "Connection lost\n";
                }
                else
                {
                    write_error("Read header error", ec);
                }

                schedule_reconnect();
                return;
            }

            // validate header
            SProtocolHeader hdr = m_header;
            if (net_to_host_u16(hdr.signature) != PROTOCOL_HEADER_SIGNATURE)
            {
                std::cerr << "Bad signature in header\n";
                schedule_reconnect();
                return;
            }

            if (hdr.version != 1)
            {
                std::cerr << "Bad version\n";
                schedule_reconnect();
                return;
            }


            uint8_t msg_num = m_cnt_packet % 256; // in header msg_num has type uint8_t !

            if (hdr.msg_num != msg_num)
            {
                std::cerr << "Bad header_msg_num = " << static_cast<unsigned int>(hdr.msg_num) << " waiting msg_num = " << static_cast<unsigned int>(msg_num) << "\n";
                schedule_reconnect();
                return;
            }

            m_cnt_packet++;

            uint32_t len = net_to_host_u32(hdr.len);

            // sanity cap
            if (len > 10 * 1024 * 1024)
            {
                std::cerr << "Packet too big, closing\n";
                schedule_reconnect();
                return;
            }

            start_read_body(len, hdr.data_type);
        });
}

void Client::start_read_body(uint32_t len, uint8_t data_type)
{
    m_body.resize(len);
    if (len == 0)
    {
        process_body(data_type, m_body);
        start_read_header();
        return;
    }

    asio::async_read(m_socket, asio::buffer(m_body),
        [this, data_type](const error_code& ec, std::size_t /*n*/)
        {
            //if (ec == asio::error::operation_aborted)
            //{
            //    return;
            //}

            if (ec)
            {
                write_error("Read body error", ec);
                schedule_reconnect();
                return;
            }

            process_body(data_type, m_body);
            start_read_header();
        });
}

void Client::process_body(uint8_t data_type, const std::vector<uint8_t>& body)
{
    static int w_cnt = 0;

    if (data_type == 0x02)
    {
        size_t pos = 0;

        if (pos + 4 > body.size())
        {
            std::cout << "No cont_whale\n";
            return;
        }

        uint32_t cnt;
        std::memcpy(&cnt, body.data() + pos, 4);
        cnt = net_to_host_u32(cnt);
        pos += 4;

        for (int i = 0; i < cnt; i++)
        {
            uint64_t ubits;

            // price
            if (pos + 8 > body.size())
            {
                std::cout << "No price\n";
                return;
            }

            std::memcpy(&ubits, body.data() + pos, 8);
            ubits = net_to_host_u64(ubits);
            pos += 8;

            double price;
            std::memcpy(&price, &ubits, sizeof(price));

            //quantity
            if (pos + 8 > body.size())
            {
                std::cout << "No quantity\n";
                return;
            }

            std::memcpy(&ubits, body.data() + pos, 8);
            ubits = net_to_host_u64(ubits);
            pos += 8;

            double quantity;
            std::memcpy(&quantity, &ubits, sizeof(quantity));

            //is_sell
            if (pos + 1 > body.size())
            {
                std::cout << "No is_sell\n";
                return;
            }
            uint8_t is_sell = body[pos++];

            //timestamp
            if (pos + 8 > body.size())
            {
                std::cout << "No timestamp\n";
                return;
            }
            uint64_t timestamp;
            std::memcpy(&timestamp, body.data() + pos, 8); 
            timestamp = net_to_host_u32(timestamp);
            pos += 8;

            //symbol name_len
            if (pos + 2 > body.size())
            {
                std::cout << "No symbol_length\n";
                return;
            }
            uint16_t symb_len;
            std::memcpy(&symb_len, body.data() + pos, 2);
            symb_len = net_to_host_u16(symb_len);
            pos += 2;

            //symbol
            if (pos + symb_len > body.size())
            {
                std::cout << "No symbol_data\n";
                return;
            }

            std::string symbol((char*)body.data() + pos, symb_len);
            pos += symb_len;

            //vwap_sess
            if (pos + 8 > body.size())
            {
                std::cout << "No vwap_sess\n";
                return;
            }

            std::memcpy(&ubits, body.data() + pos, 8);
            ubits = net_to_host_u64(ubits);
            pos += 8;

            double vwap_sess;
            std::memcpy(&vwap_sess, &ubits, sizeof(vwap_sess));

            //vwap_roll50
            if (pos + 8 > body.size())
            {
                std::cout << "No vwap_roll50\n";
                return;
            }

            std::memcpy(&ubits, body.data() + pos, 8);
            ubits = net_to_host_u64(ubits);
            pos += 8;

            double vwap_roll50;
            std::memcpy(&vwap_roll50, &ubits, sizeof(vwap_roll50));


                // not used
            ////vwap_ewma
            //if (pos + 8 > body.size())
            //{
            //    std::cout << "No vwap_ewma\n";
            //    return;
            //}

            //std::memcpy(&ubits, body.data() + pos, 8);
            //ubits = net_to_host_u64(ubits);
            //pos += 8;

            //double vwap_ewma;
            //std::memcpy(&vwap_ewma, &ubits, sizeof(vwap_ewma));


            //delta_roll
            if (pos + 8 > body.size())
            {
                std::cout << "No delta_roll\n";
                return;
            }

            std::memcpy(&ubits, body.data() + pos, 8);
            ubits = net_to_host_u64(ubits);
            pos += 8;

            double delta_roll;
            std::memcpy(&delta_roll, &ubits, sizeof(delta_roll));


                // not used
            ////delta_ewma
            //if (pos + 8 > body.size())
            //{
            //    std::cout << "No delta_ewma\n";
            //    return;
            //}

            //std::memcpy(&ubits, body.data() + pos, 8);
            //ubits = net_to_host_u64(ubits);
            //pos += 8;

            //double delta_ewma;
            //std::memcpy(&delta_ewma, &ubits, sizeof(delta_ewma));


            if (m_show_log_msg)
            {
                if(m_ext_vwap)
                    printf("\nWHALE ALERT! [%s] %s: total=%.2f price=%.2f qty==%.2f VWAP=%.2f VWAP_roll=%.2f delta_roll=%.2f \n", symbol.data(), is_sell ? "sell" : "buy", price * quantity, price, quantity, vwap_sess, vwap_roll50, delta_roll);
                else
                    printf("\nWHALE ALERT! [%s] %s: total = %.2f price = %.2f qty = %.2f VWAP = %.2f\n", symbol.data(), is_sell? "sell" : "buy",  price * quantity, price, quantity, vwap_sess);
            }

        }

    }
    else if (data_type == 0x03)
    {
        if (m_show_log_msg)
            std::cout << "Alive msg\n";
    }
    else
    {
        std::cout << "Unknown msg_data_type=" << int(data_type)  << "\n";
    }
}

void Client::schedule_reconnect()
{
    error_code ec;
    if (m_socket.is_open())
    {
        m_socket.close(ec);
    }

    m_reconnect_timer.expires_after(std::chrono::seconds(2));

    m_reconnect_timer.async_wait(
        [this](const error_code& ec)
        {
            if (ec == asio::error::operation_aborted)
            {
                return;
            }

            connect();
        });
}

void Client::clear_data()
{
    m_cnt_packet = 0;
}