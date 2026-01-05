#pragma once

#include "RingBuffer.h"
#include "CoinRegistry.h"
#include "Session.h"
#include <boost/asio.hpp>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <thread>
#include <condition_variable>
#include <deque>
#include <atomic>
#include <random>
#include <list>
#include <simdjson.h>
#include <ixwebsocket/IXNetSystem.h>
#include <ixwebsocket/IXWebSocket.h>

//constexpr size_t BUFFER_SIZE = 1 << 20;
constexpr size_t BUFFER_SIZE = 8 * 1024 * 1024;
constexpr size_t COLD_BUFFER_SIZE = 2 * 1024 * 1024;


#pragma pack(push,1)
struct MarketEvent {
    double price;
    double quantity;
    bool is_sell;
    uint64_t timestamp;
    char symbol[16];
    int index_symbol; //to index
    uint64_t symbol_hash;

    char pad[11];

    inline double total_usd() const { return price * quantity; }
};
#pragma pack(pop)
static_assert(sizeof(MarketEvent) == 64, "MarketEvent must be 64 bytes");



class Server 
{
public:

    Server(boost::asio::io_context& io, uint16_t port);
    virtual ~Server();

    // disable copying
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void Start();
    void Stop();

     // subscription
    void RegisterSession(std::shared_ptr<Session> s);
    void UnregisterExpired();

    // server API
    //void SetSignals(const VecSignal signals);
    //bool PushSignal(const Signal& s);
    //bool GetSignal(int id, Signal& s);
    //VecSignal GetSnapshot(uint8_t type);

    void EnableDataEmulation(bool is_enable) { m_data_emulation = is_enable; }
    bool IsEnableDataEmulation() { return m_data_emulation; }
    void EnableShowLogMsg(bool is_enable) { m_show_log_msg = is_enable; }
    bool IsShowLogMsg() { return m_show_log_msg; }

    boost::asio::io_context& GetIoContext() { return m_io; }

private:
    void do_accept();
    void session_dispatcher();
    void producer_loop();
    void clear_sessions();


    void binance_stream();
    void hot_dispatcher();
    void event_dispatcher();
    void speed_monitor();
    //void client_update_timer();
    void parse_single_event(simdjson::dom::element item);
    void process_market_msg(const ix::WebSocketMessagePtr& msg);

    void register_coins();


protected:

    boost::asio::io_context& m_io;
    boost::asio::ip::tcp::acceptor m_acceptor;

    std::mutex m_mtx_subscribers;
    //std::list<std::weak_ptr<Session>> m_subscribers;
    std::list<std::shared_ptr<Session>> m_subscribers;

    //std::mutex m_mtx_state;
    //std::unordered_map<uint32_t, Signal> m_state;

    RingBuffer<MarketEvent, BUFFER_SIZE>  m_hot_buffer;
    //SPSCRingBuffer<MarketEvent, BUFFER_SIZE> m_hot_buffer;

    RingBuffer<WhaleEvent, COLD_BUFFER_SIZE> m_event_buffer;

    CoinRegistry m_reg_coin;

    // signal event queue
    std::mutex m_mtx_queue;
    std::condition_variable m_cv_queue;
    std::deque<Signal> m_queue;
    std::atomic<bool> m_running{ true };

    std::thread m_session_dispatcher;
    std::thread m_producer;
    std::thread m_hot_dispatcher;
    std::thread m_monitor;
    std::thread m_event_dispatcher;

    std::atomic<bool> m_data_emulation{ true };
    std::atomic<bool> m_show_log_msg{ true };

    std::atomic<bool> m_need_update_clients{ false };

};

//static_assert(sizeof(Server::MarketEvent) == 64, "MarketEvent must be 64 bytes");