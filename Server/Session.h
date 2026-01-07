#pragma once

#include "RingBuffer.h"
//#include "CoinRegistry.h"
#include <Protocol.h>
#include <boost/asio.hpp>
#include <deque>
#include <vector>
#include <memory>
#include <chrono>


class Server;

constexpr size_t SESSION_BUFFER_SIZE = 512*1024;


#pragma pack(push,1)
struct WhaleEvent {

    double price;
    double quantity;

    bool is_sell;
    uint64_t timestamp;
    int index_symbol;

    double vwap_sess;
    double vwap_roll50;
    double vwap_ewma;

    float delta_roll;
    float reserve; //float delta_ewma; // not used
    

    char pad[3];

    inline double total_usd() const { return price * quantity; }

};
#pragma pack(pop)
static_assert(sizeof(WhaleEvent) == 64, "WhaleEvent must be 64 bytes");



class Session : public std::enable_shared_from_this<Session> 
{
    using tcp = boost::asio::ip::tcp;

public:
    Session(tcp::socket socket, Server& server);
    ~Session();

    void Start();
    //void DeliverUpdates(const VecSignal& updates);
    void DeliverUpdates(std::vector<WhaleEvent>& events, size_t size);
    bool Expired() const;
    void ForceClose();
    void PushEvent(const WhaleEvent& event);

    inline double GetWhaleTreshold() { return m_whale_treshold; };
    inline int GetSymbolIndex() { return m_ind_symb; };

private:
    void async_read_header();
    void async_read_body(std::size_t len, uint8_t data_type);
    void handle_subscribe(const std::vector<uint8_t>& payload);
    void do_write();
    void close();

    void event_reader();

private:
    using SocketExecutor = boost::asio::ip::tcp::socket::executor_type;
    using SessionStrand = boost::asio::strand<SocketExecutor>;
    using time_point = std::chrono::steady_clock::time_point;

    tcp::socket m_socket;

    SessionStrand m_strand;

    Server& m_server;

    std::array<uint8_t, sizeof(SProtocolHeader)> m_buf_header;
    std::vector<uint8_t> m_buf_body;

    std::deque<std::shared_ptr<std::vector<uint8_t>>> m_que_write;

    uint8_t m_req_type{ 0 };

    uint8_t m_msg_num{ 0 };

    time_point m_time_last_send;

    //std::shared_ptr<Session> m_self;          // keep the self-pointer while the session is active
    std::atomic<bool> m_closing{ false };

    //

    SPSCRingBuffer<WhaleEvent, SESSION_BUFFER_SIZE> m_event_buffer;
    std::thread m_event_dispatcher;

public:
    //std::string m_coin_name{"BTCUSDT"};
    //uint64_t m_coin_name_hash{0};
    double m_whale_treshold{ 105000 };
    int m_ind_symb{ 0 };

};
