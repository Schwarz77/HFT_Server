#pragma once

#include "RingBuffer.h"
#include "CoinRegistry.h"
#include "Analytics.h"
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
#include <xcall_once.h>

#include <simdjson.h>
#include <ixwebsocket/IXNetSystem.h>
#include <ixwebsocket/IXWebSocket.h>


constexpr size_t BUFFER_SIZE = 8 * 1024 * 1024;
constexpr size_t COLD_BUFFER_SIZE = 2 * 1024 * 1024;


#pragma pack(push,1)
struct MarketEvent {
    double price;
    double quantity;
    bool is_sell;
    uint64_t timestamp;
    char reserved[16]; //char symbol[16]; // not used
    int index_symbol;
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

    void EnableDataEmulation(bool is_enable) { m_data_emulation.store(is_enable, std::memory_order::release); };
    bool IsEnableDataEmulation() { return m_data_emulation.load(std::memory_order::acquire); }

    void EnableShowLogMsg(bool is_enable) { m_show_log_msg = is_enable; }
    bool IsShowLogMsg() { return m_show_log_msg; }

    void SetExtCalcVWAP(bool is_ext) { m_ext_vwap.store(is_ext, std::memory_order::release); };
    bool IsExtCalcVWAP() { return m_ext_vwap.load(std::memory_order::acquire); }

    boost::asio::io_context& GetIoContext() { return m_io; }

    std::string GetCoinSymbol(int index) const;
    int GetCoinIndex(std::string& symbol) const;

private:
    void do_accept();
    void session_dispatcher();
    void clear_sessions();

    void producer();
    void emulator_loop();
    void binance_stream();
    void hot_dispatcher();
    void event_dispatcher();
    void speed_monitor();
    void parse_single_event(simdjson::dom::element item);
    void process_market_msg(const ix::WebSocketMessagePtr& msg);

    void register_coins();
    void init_coin_data();

protected:

    boost::asio::io_context& m_io;
    boost::asio::ip::tcp::acceptor m_acceptor;

    std::mutex m_mtx_subscribers;
    std::vector<std::shared_ptr<Session>> m_subscribers;

    RingBuffer<MarketEvent, BUFFER_SIZE>  m_hot_buffer;
    RingBuffer<WhaleEvent, COLD_BUFFER_SIZE> m_event_buffer;

    CoinRegistry m_reg_coin;

    std::atomic<bool> m_running{ true };

    std::thread m_session_dispatcher;
    std::thread m_producer;
    std::thread m_hot_dispatcher;
    std::thread m_monitor;
    std::thread m_event_dispatcher;

    std::atomic<bool> m_data_emulation{ true };
    std::atomic<bool> m_ext_vwap{ false };
    std::atomic<bool> m_show_log_msg{ true };
    std::atomic<bool> m_need_update_clients{ true };

    //size_t COIN_CNT{0};
    //std::vector<CoinPair> m_coins;
    //std::vector<double> m_whale_global_treshold;
    //std::vector<CoinAnalytics> m_coin_VWAP;
    std::once_flag m_coins_initialized;

};
