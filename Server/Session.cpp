// Session.cpp

#include "Session.h"
#include "Server.h"
#include <iostream>
#include <cstring>
#include <cassert>
#include <Utils.h>

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;
using time_point = std::chrono::steady_clock::time_point;
using steady_clock = std::chrono::steady_clock;


Session::Session(tcp::socket socket, Server& server)
    : m_socket(std::move(socket))
    , m_strand(asio::make_strand(m_socket.get_executor()))
    , m_server(server)
{
    m_time_last_send = steady_clock::now();
    m_event_dispatcher = std::thread(&Session::event_reader, this);
}

Session::~Session()
{
}

void Session::Start()
{
    //m_self = shared_from_this(); // Holding a shared_ptr (self)

    async_read_header();
}

void Session::async_read_header()
{
    if (!m_socket.is_open())
    {
        return;
    }

    auto self = shared_from_this();

    // read exactly header size
    asio::async_read(m_socket, asio::buffer(m_buf_header),
        asio::bind_executor(m_strand,
            [this, self](error_code ec, std::size_t /*n*/)
            {
                if (ec)
                {
                    if (ec == asio::error::connection_reset ||
                        ec == asio::error::eof)
                    {
                        if (m_server.IsShowLogMsg())
                            std::cout << "\nClient disconnected\n";
                    }
                    else if (ec == asio::error::operation_aborted)
                    {
                        //std::cout << "Read header aborted (server disconnects clients in Server::SetSignals)\n";
                    }
                    else 
                    {
                        write_error("Read header error", ec);
                    }

                    close();
                    return;
                }


                SProtocolHeader hdr;
                std::memcpy(&hdr, m_buf_header.data(), sizeof(hdr));

                if (net_to_host_u16(hdr.signature) != PROTOCOL_HEADER_SIGNATURE)
                {
                    std::cerr << "\nSession: bad signature, closing\n";
                    close();
                    return;
                }

                if (hdr.version != 1)
                {
                    std::cerr << "\nSession: bad version, closing\n";
                    close();
                    return;
                }

                if (hdr.msg_num != 0)
                {
                    std::cerr << "\nSession: bad msg_num, closing\n";
                    close();
                    return;
                }

                uint8_t data_type = hdr.data_type;
                uint32_t len = net_to_host_u32(hdr.len);

                if (len > 10 * 1024 * 1024)
                {
                    std::cerr << "\nSession: payload too large (" << len << "), closing\n";
                    close();
                    return;
                }

                ///
                // tmp emulate ETH client
                static int cnt_gen = 0;
                if (cnt_gen++ % 4 == 1)
                {
                    m_whale_treshold = 125000;
                    m_ind_symb = 1;//ETH
                }
                //
                ///

                async_read_body(len, data_type);

            }));
}

void Session::async_read_body(std::size_t len, uint8_t data_type)
{
    if (!m_socket.is_open())
    {
        return;
    }

    auto self = shared_from_this();

    if (len)
        m_buf_body.resize(len);
    else
        m_buf_body.clear();

    asio::async_read(m_socket, asio::buffer(m_buf_body),
        asio::bind_executor(m_strand,
            [this, self, data_type, len](error_code ec, std::size_t /*n*/)
            {
                if (ec)
                {
                    if (ec == asio::error::connection_reset ||
                        ec == asio::error::eof)
                    {
                        if (m_server.IsShowLogMsg())
                            std::cout << "\nClient disconnected\n";
                    }
                    else if (ec == asio::error::operation_aborted)
                    {
                        //std::cout << "\nRead body aborted (server disconnects clients in Server::SetSignals)\n";
                    }
                    else
                    {
                        write_error("Read body error", ec);
                    }
                    close();
                    return;
                }

                if (data_type == 0x01)
                {
                    handle_subscribe(m_buf_body);
                }
                else
                {
                    std::cerr << "\nSession: unexpected dataType from client: " << int(data_type) << "\n";
                }

                async_read_header(); // it need for work without m_self

            }));
}

void Session::handle_subscribe(const std::vector<uint8_t>& payload)
{
    if (payload.empty())
    {
        std::cerr << "\nSession: subscribe payload empty\n";
        close();
        return;
    }

    m_req_type = payload[0];

    if (m_server.IsShowLogMsg())
        std::cout << "\nSession: client subscribed to type=" << int(m_req_type) << "\n";

    m_server.RegisterSession(shared_from_this());

    //// send initial snapshot for this type
    //auto snap = m_server.GetSnapshot(m_req_type);
    //if (!snap.empty())
    //{
    //    DeliverUpdates(snap);
    //}

    do_write();
}


void Session::DeliverUpdates(std::vector<WhaleEvent>& events, size_t size)
{
    auto self = shared_from_this();
    asio::post(m_strand, [this, self, events, size]()
        {
            if (!m_socket.is_open())
            {
                return;
            }

            std::vector<uint8_t> payload;
            payload.reserve(size * (sizeof(WhaleEvent)) + 4);

            // count
            uint32_t count = host_to_net_u32(static_cast<uint32_t>(size));
            payload.insert(payload.end(), (uint8_t*)&count, (uint8_t*)&count + 4);

            for (int i=0; i<size; i++ )
            {
                //Signal& e = events[i];
                const WhaleEvent& we = events[i];


                // price
                uint64_t price;
                static_assert(sizeof(price) == sizeof(we.price), "double size mismatch");
                std::memcpy(&price, &we.price, sizeof(price));
                price = host_to_net_u64(price);
                payload.insert(payload.end(), (uint8_t*)&price, (uint8_t*)&price + 8);


                //quantity
                uint64_t quantity;
                static_assert(sizeof(quantity) == sizeof(we.quantity), "double size mismatch");
                std::memcpy(&quantity, &we.quantity, sizeof(quantity));
                quantity = host_to_net_u64(quantity);
                payload.insert(payload.end(), (uint8_t*)&quantity, (uint8_t*)&quantity + 8);

                //is_sell
                payload.push_back(static_cast<uint8_t>(we.is_sell));


                //timestamp
                uint64_t timestamp;
                static_assert(sizeof(timestamp) == sizeof(we.timestamp), "double size mismatch");
                std::memcpy(&timestamp, &we.timestamp, sizeof(timestamp));
                timestamp = host_to_net_u64(timestamp);
                payload.insert(payload.end(), (uint8_t*)&timestamp, (uint8_t*)&timestamp + 8);

                //symbol name
                std::string symbol = m_server.GetCoinSymbol(we.index_symbol);
                uint16_t str_len = symbol.length();
                str_len = host_to_net_u16(str_len);
                payload.insert(payload.end(), (uint8_t*)&str_len, (uint8_t*)&str_len + 2);
                payload.insert(payload.end(), (uint8_t*)symbol.data(), (uint8_t*)symbol.data() + symbol.length());

                //vwap_sess
                uint64_t vwap_sess;
                static_assert(sizeof(vwap_sess) == sizeof(we.vwap_sess), "double size mismatch");
                std::memcpy(&vwap_sess, &we.vwap_sess, sizeof(vwap_sess));
                vwap_sess = host_to_net_u64(vwap_sess);
                payload.insert(payload.end(), (uint8_t*)&vwap_sess, (uint8_t*)&vwap_sess + 8);

                //vwap_roll50
                uint64_t vwap_roll50;
                static_assert(sizeof(vwap_roll50) == sizeof(we.vwap_roll50), "double size mismatch");
                std::memcpy(&vwap_roll50, &we.vwap_roll50, sizeof(vwap_roll50));
                vwap_roll50 = host_to_net_u64(vwap_roll50);
                payload.insert(payload.end(), (uint8_t*)&vwap_roll50, (uint8_t*)&vwap_roll50 + 8);

                //vwap_ewma
                uint64_t vwap_ewma;
                static_assert(sizeof(vwap_ewma) == sizeof(we.vwap_ewma), "double size mismatch");
                std::memcpy(&vwap_ewma, &we.vwap_ewma, sizeof(vwap_ewma));
                vwap_ewma = host_to_net_u64(vwap_ewma);
                payload.insert(payload.end(), (uint8_t*)&vwap_ewma, (uint8_t*)&vwap_ewma + 8);

                //delta_roll
                double delta_roll_src = we.delta_roll;
                uint64_t delta_roll;
                static_assert(sizeof(delta_roll) == sizeof(delta_roll_src), "double size mismatch");
                std::memcpy(&delta_roll, &delta_roll_src, sizeof(delta_roll));
                delta_roll = host_to_net_u64(delta_roll);
                payload.insert(payload.end(), (uint8_t*)&delta_roll, (uint8_t*)&delta_roll + 8);


                //delta_ewma
                double delta_ewma_src = we.delta_ewma;
                uint64_t delta_ewma;
                static_assert(sizeof(delta_ewma) == sizeof(delta_ewma_src), "double size mismatch");
                std::memcpy(&delta_ewma, &delta_ewma_src, sizeof(delta_ewma));
                delta_ewma = host_to_net_u64(delta_ewma);
                payload.insert(payload.end(), (uint8_t*)&delta_ewma, (uint8_t*)&delta_ewma + 8);

            }

            SProtocolHeader hdr;
            hdr.signature = host_to_net_u16(PROTOCOL_HEADER_SIGNATURE);
            hdr.version = 1;
            hdr.data_type = 0x02;
            hdr.msg_num = m_msg_num++;
            hdr.len = host_to_net_u32(static_cast<uint32_t>(payload.size()));

            auto frame = std::make_shared<std::vector<uint8_t>>();
            frame->resize(sizeof(hdr) + payload.size());
            std::memcpy(frame->data(), &hdr, sizeof(hdr));
            if (!payload.empty())
            {
                std::memcpy(frame->data() + sizeof(hdr), payload.data(), payload.size());
            }

            bool need_start = m_que_write.empty() /*&& !m_writing*/;
            m_que_write.push_back(frame);
            if (need_start)
            {
                do_write();
            }

            m_time_last_send = steady_clock::now();
        });
}

void Session::do_write()
{
    if (!m_socket.is_open())
    {
        m_que_write.clear();
        return;
    }

    if (m_que_write.empty())
    {
        return;
    }

    auto frame = m_que_write.front();
    auto self = shared_from_this();

    asio::async_write(m_socket, asio::buffer(*frame),
        asio::bind_executor(m_strand,
            [this, self, frame](error_code ec, std::size_t /*n*/) 
            {
                if (ec)
                {
                    if (ec == asio::error::connection_reset ||
                        ec == asio::error::eof)
                    {
                        if (m_server.IsShowLogMsg())
                            std::cout << "\nClient disconnected\n";
                    }
                    else if(ec == asio::error::operation_aborted)
                    {
                        //std::cout << "\nWrite aborted (server disconnects clients in Server::SetSignals)\n";
                    }
                    else
                    {
                        write_error("Write error", ec);
                    }

                    close();
                    return;
                }

                // remove sent frame and continue
                m_que_write.pop_front();
                if (!m_que_write.empty())
                {
                    do_write();
                }
            }));
}

void Session::close()
{
    if (m_closing.exchange(true))
        return;

    error_code ec;
    if (m_socket.is_open())
    {
        boost::asio::socket_base::linger option(true, 0);
        m_socket.set_option(option);

        m_socket.shutdown(asio::socket_base::shutdown_both, ec);
        m_socket.close(ec);
    }

    if (m_event_dispatcher.joinable())
    {
        m_event_dispatcher.join();
    }

    // clear queued frames on the strand to avoid races
    auto self = shared_from_this();
    asio::post(m_strand, [this, self]() 
        {
            m_que_write.clear();

            //m_self.reset();
        });

    //std::cout << "\nSession closed\n";
}

bool Session::Expired() const
{
    return !m_socket.is_open();
}

void Session::ForceClose()
{
    auto self = shared_from_this();
    asio::post(m_strand, [this, self]()
        {
            close();
        });
}

void Session::PushEvent(const WhaleEvent& event)
{
    //static int cnt = 0; 

    if (!m_event_buffer.try_push(event))
    {
        //cnt++;

        //if (cnt > 10)
        //{
        //    cnt = 0;
        //    std::cout << std::endl << "Session::PushEvent - DROP: " << cnt << std::endl;
        //}
    }
}

void Session::event_reader() {
    //SetThreadAffinityMask(GetCurrentThread(), 1 << 6);
    //SetThreadAffinityMask(GetCurrentThread(), 1 << 2);

    int empty_cycles = 0;
    const size_t size_batch = 4096;
    std::vector<WhaleEvent> batch(size_batch);

    int w_cnt = 0;

    uint64_t reader_idx = m_event_buffer.get_head();

    while (m_socket.is_open()) {
        size_t total_processed_in_this_tick = 0;
        size_t read_count = 0;

        //////
        //uint64_t h = m_event_buffer.get_head(); // memory_order_acquire

        //if (h - reader_idx > m_event_buffer.capacity() * 0.9) {
        //    reader_idx = h;
        //    //m_event_buffer.update_tail(reader_idx);
        //    printf("\nSession::event_reader OVERLOADED! DROPS!\n");
        //}
        //////         


        while ((read_count = m_event_buffer.pop_batch(&batch[0], size_batch)) > 0) {
            total_processed_in_this_tick += read_count;

            reader_idx += read_count;


            DeliverUpdates(batch, read_count);

        }

        if (total_processed_in_this_tick > 0) {
            empty_cycles = 0;
            _mm_pause();
        }
        else {
            empty_cycles++;
            if (empty_cycles < 1000) {
                _mm_pause();
            }
            else if (empty_cycles < 50000) {
                for (int j = 0; j < 10; ++j) _mm_pause();
            }
            else if (empty_cycles < 100000) {
                std::this_thread::yield();
            }
            else {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }
}