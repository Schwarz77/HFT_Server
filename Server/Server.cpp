// server.cpp

#include "Server.h"
#include "Session.h"
#include <iostream>
#include <chrono>
#include <Utils.h>
#include "Analytics.h"

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;
using time_point = std::chrono::steady_clock::time_point;
using steady_clock = std::chrono::steady_clock;


///////////////////////////////////////////////////////////////////////


const CoinPair coin_data[] =
{
    {"BTCUSDT", 96000.0}, {"ETHUSDT", 2700.0}, {"SOLUSDT", 180.0}, {"BNBUSDT", 600.0}
};

const size_t COIN_CNT = _countof(coin_data);

double whale_treshold[COIN_CNT] = { 100000, 70000, 50000, 60000 };

//CoinVWAP coin_VWAP[COIN_CNT];

CoinAnalytics coin_VWAP[COIN_CNT];



///////////////////////////////////////////////////////////////////////


void set_affinity(std::thread& t, int logical_core_id) 
{
    HANDLE handle = t.native_handle();

    DWORD_PTR mask = (1ULL << logical_core_id);
    if (SetThreadAffinityMask(handle, mask) == 0) 
    {
        std::cerr << "\nError setting affinity\n";
    }
}

Server::Server(asio::io_context& io, uint16_t port)
    : m_io(io), m_acceptor(io, tcp::endpoint(tcp::v4(), port), true/*false*/)
{
    //Start();

    register_coins();

}

Server::~Server() 
{
    Stop();
}

void Server::Start() 
{
    m_session_dispatcher = std::thread(&Server::session_dispatcher, this);
    //m_producer = std::thread(&Server::producer_loop, this);
    m_hot_dispatcher = std::thread(&Server::hot_dispatcher, this);
    m_event_dispatcher = std::thread(&Server::event_dispatcher, this);
    m_monitor = std::thread(&Server::speed_monitor, this);
    m_producer = std::thread(&Server::producer, this);

    set_affinity(m_producer, 0);
    //set_affinity(m_session_dispatcher, 2);
    set_affinity(m_hot_dispatcher, 2);
    set_affinity(m_event_dispatcher, 4);
    set_affinity(m_monitor, 5);  // tail to 3 core
    
    do_accept();

    if (m_show_log_msg)
        std::cout << "Server started\n";
}

void Server::do_accept() 
{
    m_acceptor.async_accept([this](error_code ec, tcp::socket socket) 
        {
            if (!ec) 
            {
                if (m_show_log_msg)
                    std::cout << "\nAccepted connection\n";

                auto s = std::make_shared<Session>(std::move(socket), *this);
                s->Start();

                do_accept();
            }
            else if (ec == boost::asio::error::operation_aborted)
            {
                // Normal termination (cancellation)
                
                //std::cout << "Acceptor stopped gracefully." << std::endl;
            }
            else if (   ec == boost::asio::error::would_block   ||
                        ec == boost::asio::error::interrupted)
            {
                // Recoverable errors (need to try again).

                write_error("Accept error", ec);
                do_accept();
            }
            else
            {
                // Unrecoverable error (including 10009 Bad File Descriptor)

                write_error("Accept error, STOP ACCEPT!", ec);

                // do_accept() should never be called !

            }
        });
}

void Server::Stop() 
{
    m_running = false;

    // close acceptor
    error_code ec;

    if (m_acceptor.is_open())
    {
        m_acceptor.cancel(ec);
    }

    if (m_acceptor.is_open())
    {
        m_acceptor.close(ec);
    }

    if (m_producer.joinable())
    {
        m_producer.join();
    }

    if (m_session_dispatcher.joinable())
    {
        m_session_dispatcher.join();
    }

    if (m_monitor.joinable())
    {
        m_monitor.join();
    }

    if (m_hot_dispatcher.joinable())
    {
        m_hot_dispatcher.join();
    }

    if (m_event_dispatcher.joinable())
    {
        m_event_dispatcher.join();
    }

    clear_sessions(); 
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

}


void Server::RegisterSession(std::shared_ptr<Session> s) 
{
    {
        std::lock_guard<std::mutex> lk(m_mtx_subscribers);

        m_subscribers.push_back(s);
    }

    m_need_update_clients.store(true, std::memory_order_release);

}

void Server::UnregisterExpired() 
{
    bool is_changed = false;

    {
        std::lock_guard<std::mutex> lk(m_mtx_subscribers);

        m_subscribers.erase(std::remove_if(m_subscribers.begin(), m_subscribers.end(),
            [&is_changed](const std::shared_ptr<Session>& s)
            {
                if (s->Expired()) {
                    is_changed = true;
                    return true;
                }
                return false;
            }),
            m_subscribers.end());
    }

    if (is_changed)
    {
        m_need_update_clients.store(true, std::memory_order_release);
    }
}


void Server::session_dispatcher() 
{
    while (m_running) 
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        UnregisterExpired();

    }
}

///////////////////////////////////
//

void Server::register_coins()
{
    //for (int i = 0; i < COIN_CNT; i++)
    //{
    //    m_reg_coin.register_coin(coin_data[i].symbol, i);
    //}

    std::lock_guard<std::mutex> lk_symb(m_mtx_coin_symbol);

    for (int i = 0; i < COIN_CNT; i++)
    {
        m_mapCoinInd2Symbol[i] = coin_data[i].symbol;
        m_mapCoinSymbol2Ind[coin_data[i].symbol] = i;
    }
}

std::string Server::GetCoinSymbol(int index)
{
    std::lock_guard<std::mutex> lk_symb(m_mtx_coin_symbol);

    auto it = m_mapCoinInd2Symbol.find(index);
    if (it != m_mapCoinInd2Symbol.end())
        return it->second;

    return std::string();
}

int Server::GetCoinIndex(std::string& symbol)
{
    std::lock_guard<std::mutex> lk_symb(m_mtx_coin_symbol);

    auto it = m_mapCoinSymbol2Ind.find(symbol);
    if (it != m_mapCoinSymbol2Ind.end())
        return it->second;

    return -1;
}

void Server::producer()
{
    SetThreadAffinityMask(GetCurrentThread(), 1 << 0);

    if (m_data_emulation.load(std::memory_order_acquire))
    {
        emulator_loop();
    }
    else
    {
        binance_stream();

        //while (m_running)
        //{
        //    binance_stream();

        //    std::this_thread::sleep_for(std::chrono::milliseconds(100));
        //}
    }
}

void Server::emulator_loop()
{
    const size_t size_batch = 64;
    std::vector<MarketEvent> batch(size_batch);

    int64_t cnt = 0;
    int64_t cnt_whale = 0;

    int64_t cnt_whale_gen = 0;
    int64_t cnt_tm_upd = 0;

    uint64_t batch_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    uint64_t m_dropped_producer = 0;


    while (m_running) {

        if (m_hot_buffer.can_write(size_batch))
        {
            for (int i = 0; i < size_batch; ++i) {
                //int ind = i % COIN_CNT;
                int ind = fast_range(COIN_CNT - 1);

                auto& t = coin_data[ind];

                // time
                if(cnt_tm_upd++ >= 50'000'000)
                {
                    cnt_tm_upd = 0;
                    batch_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()
                    ).count();
                }


                {
                    MarketEvent& ev = batch[i];

                    ev.index_symbol = ind;

                    ev.timestamp = batch_ts;

                    //std::memcpy(ev.symbol, t.symbol, 16);  need remove symbol from MarketEvent

                    ev.price = t.price + fast_float_range(0, 0.7);

                    if (cnt_whale_gen++ >= 75'000'000)
                    {
                        cnt_whale_gen = 0;
                        //ev.quantity = 100;
                        ev.quantity =       (ev.index_symbol == 0) ?     1 + fast_range(5) + fast_float_range(0, 0.99)
                                        :   (ev.index_symbol == 1) ?    40 + fast_range(200) + fast_float_range(0, 0.99)
                                        :   (ev.index_symbol == 2) ?    555 + fast_range(555*5) + fast_float_range(0, 0.99)
                                        :                               170 + fast_range(170 * 5) + fast_float_range(0, 0.99);
                    }
                    else
                    {
                        ev.quantity = 1;
                    }

                    ev.is_sell = ((i & 1) == 0); //(i % 2 == 0);


                    cnt++;
 /*                   if (ev.quantity > 99 && ev.total_usd() >= 960000.5)
                    {
                        cnt_whale++;
                    }*/
                }

            }

             m_hot_buffer.push_batch(batch.data(), size_batch);
        }
        else
        {
            m_dropped_producer += size_batch;
        }

        _mm_pause(); 

    }
}


void Server::speed_monitor()
{
    uint64_t last_head = m_hot_buffer.get_head();
    auto last_time = std::chrono::steady_clock::now();

    while (m_running) {

        std::this_thread::sleep_for(std::chrono::seconds(1));

        uint64_t current_head = m_hot_buffer.get_head();
        auto current_time = std::chrono::steady_clock::now();

        uint64_t delta = current_head - last_head;

        std::chrono::duration<double> duration = current_time - last_time;
        double seconds = duration.count();

        double speed = delta / seconds;

        double eps = (speed >= 1e6) ? speed / 1e6 : (speed >= 1e3) ? speed / 1e3 : speed;
        double total = (current_head >= 1e6) ? current_head / 1e6 : (current_head >= 1e3) ? current_head / 1e3 : current_head;
        std::string mul = (speed >= 1e6) ? " M" : (speed >= 1e3) ? " K" : "";

        std::stringstream ss;
        ss << std::fixed << std::setprecision(2) << eps << mul << " event/sec | " << "Total: " << current_head << " events";

        std::cout << "\r" << "Throughput: " << std::left << std::setw(50) << ss.view() /*ss.str()*/ << std::flush;

        last_head = current_head;
        last_time = current_time;
    }
}

/// </summary>

void Server::clear_sessions()
{
    {
        std::lock_guard<std::mutex> lk(m_mtx_subscribers);

        for (auto& sp : m_subscribers)
        {
            if (!sp->Expired())
            {
                sp->ForceClose();
            }
        }

        m_subscribers.clear();
    }

    m_need_update_clients.store(true, std::memory_order_release);

}

////

static inline uint64_t symbol_u64(const char* s)
{
    uint64_t v = 0;
    std::memcpy(&v, s, 8);
    return v;
}

inline void Server::parse_single_event(simdjson::dom::element item) 
{
    MarketEvent event;

    static uint64_t btc = symbol_u64("BTCUSDT");
    static uint64_t eth = symbol_u64("ETHUSDT");
    static uint64_t sol = symbol_u64("SOLUSDT");
    static uint64_t bnb = symbol_u64("BNBUSDT");

    // 's' - it's deal
    std::string_view s;
    if (item["s"].get(s) == simdjson::error_code::SUCCESS) {
        // Timestamp
        event.timestamp = item["E"].get_uint64();

        // Symbol
        size_t len = std::min<size_t>(s.size(), sizeof(event.symbol) - 1);
        if (len < 16)
        {
            std::memcpy(event.symbol, s.data(), len);
            event.symbol[len] = '\0';
        }
        else
        {
            int ddd = 0;
        }

        ///// tmp
        uint64_t s64 = symbol_u64(event.symbol);

        if(s64 == btc)
            event.index_symbol = 0;
        else if (s64 == eth)
            event.index_symbol = 1;
        else if (s64 == sol)
            event.index_symbol = 2;
        else if (s64 == bnb)
            event.index_symbol = 3;
        else
            event.index_symbol = 0; 
        //////


        // Price & Quantity (строки в JSON -> double)
        std::string_view p_str = item["p"].get_string();
        std::from_chars(p_str.data(), p_str.data() + p_str.size(), event.price);

        std::string_view q_str = item["q"].get_string();
        std::from_chars(q_str.data(), q_str.data() + q_str.size(), event.quantity);

        // Side
        event.is_sell = item["m"].get_bool();

        if (event.timestamp > 0) {
            m_hot_buffer.push_batch(&event, 1);
        }
        else
        {
            int ddd = 0;
        }
    }
}

void Server::process_market_msg(const ix::WebSocketMessagePtr& msg) {
    static thread_local simdjson::dom::parser dom_parser;
    try {
        simdjson::dom::element root = dom_parser.parse(msg->str);

        if (root.is_array()) {
            for (auto item : root.get_array()) parse_single_event(item);
        }
        else if (root.is_object()) {
            simdjson::dom::element data_field;
            auto error = root["data"].get(data_field);

            if (!error) {
                if (data_field.is_array()) {
                    for (auto item : data_field.get_array()) parse_single_event(item);
                }
                else {
                    parse_single_event(data_field);
                }
            }
            else {
                parse_single_event(root);
            }
        }
    }
    catch (...) {
        int ddd = 0;
    }
}

void Server::binance_stream()
{
    ix::initNetSystem();

    std::string url = "wss://fstream.binance.com/ws";

    while (m_running) // RECONNECT LOOP
    {
        ix::WebSocket ws;
        ws.setUrl(url);

        //ws.disableAutomaticReconnection(); 

        std::atomic<uint64_t> ws_in_cnt{ 0 };

        ws.setOnMessageCallback(
            [&](const ix::WebSocketMessagePtr& msg)
            {
                switch (msg->type)
                {
                case ix::WebSocketMessageType::Open:
                {
                    std::cout << "\n[Binance] Connected\n";

                    std::string sub = R"({
                        "method": "SUBSCRIBE",
                        "params": [
                            "btcusdt@trade",
                            "ethusdt@trade",
                            "solusdt@trade",
                            "bnbusdt@trade"
                        ],
                        "id": 1
                    })";

                    ws.send(sub);
                    break;
                }

                case ix::WebSocketMessageType::Message:
                {
                    ws_in_cnt.fetch_add(1, std::memory_order_relaxed);

                    process_market_msg(msg);

                    break;
                }

                case ix::WebSocketMessageType::Ping:
                {
                    //ws.pong(msg->str);
                    break;
                }

                case ix::WebSocketMessageType::Pong:
                    break;

                case ix::WebSocketMessageType::Close:
                {
                    std::cerr << "\n[Binance] Closed by server\n";
                    //ws.stop();
                    return;
                }

                case ix::WebSocketMessageType::Error:
                {
                    std::cerr << "\n[Binance] Error: "
                        << msg->errorInfo.reason << "\n";
                    //ws.stop();
                    return;
                }

                default:
                    break;
                }
            });


        ws.setPingInterval(15);

        ws.start();


        uint64_t last = 0;
        while (m_running)
        {
            std::this_thread::sleep_for(std::chrono::seconds(5));

            uint64_t cur = ws_in_cnt.load();
            if (cur == last)
            {
                //std::cerr << "\n[Binance] No data,  reconnect\n";
                //ws.stop();
                break;
            }
            last = cur;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cerr << "\n[Binance] Reconnecting...\n";

        // TODO: set flag to reset VWAP_session & VWAP_emwa
        // ..
    }
}


void Server::hot_dispatcher() 
{
    SetThreadAffinityMask(GetCurrentThread(), 1 << 2);

    uint64_t reader_idx = m_hot_buffer.get_head();
    const int threshold_test = 5000;

    int empty_cycles = 0;
    uint64_t iter_count = 0;
    uint64_t total_dropped = 0;

    std::vector<WhaleEvent> bach_to_client(1024);
    uint64_t cnt_event_to_client = 0;

    bool ext_vwap = m_ext_vwap.load(std::memory_order::acquire);

    while (m_running) {
        uint64_t h = m_hot_buffer.get_head();

        size_t avail_read = h - reader_idx;

        //
        if (avail_read > m_hot_buffer.capacity() * 0.9) {
            // We are falling behind, it's drops
            reader_idx = h;
            m_hot_buffer.update_tail(reader_idx);
            printf("\nhot_dispatcher OVERLOADED! DROPS!\n");

        }
        //

        if (h <= reader_idx) {
            _mm_pause();
            continue;
        }

        // Параметры пачки
        //size_t to_process = std::min<size_t>(h - reader_idx, 1024);
        size_t to_process = (avail_read < 1024) ? avail_read : 1024;
    

        uint64_t first_sec = 0;
        uint64_t last_sec = 0;
        bool first = true;

        for (size_t i = 0; i < to_process; ++i) {
            const auto& ev = m_hot_buffer.read(reader_idx++);

            if (ev.index_symbol < 0  || ev.index_symbol >= COIN_CNT)
                continue;

            double pv = ev.total_usd();

            uint64_t sec = ev.timestamp / 1000;

            auto& c = coin_VWAP[ev.index_symbol];

            //if (need_reset_vwap.load())
            //{
            //    c.session.reset();
            //    c.signed_flow = 0;
            ///    if (IsExtCalcVWAP())
            ////        c.ewma.reset();
            //}

            c.session.add(ev.price, ev.quantity);
            if (ext_vwap)
            {
                c.roll50.add(ev.price, ev.quantity);
                //c.ewma.add(ev.price, 0.05, ev.timestamp); //c.ewma.update(ev.price, ev.timestamp);
                //c.signed_flow += ev.is_sell ? -ev.quantity : ev.quantity;
            }

            if (pv >= whale_treshold[ev.index_symbol]) [[unlikely]]
            {
                WhaleEvent& we = bach_to_client[cnt_event_to_client++];
                we.index_symbol = ev.index_symbol;
                we.price = ev.price;
                we.quantity = ev.quantity;
                we.timestamp = ev.timestamp;
                we.is_sell = ev.is_sell;
                we.vwap_sess = c.session.value();

                if (ext_vwap)
                {
                    we.vwap_roll50 = c.roll50.value();
                    //we.vwap_ewma = c.ewma.value(); //we.vwap_ewma = c.ewma.value;
                    we.delta_roll = ev.price - we.vwap_roll50;
                    //we.delta_ewma = (we.delta_ewma != 0) ? ev.price - we.vwap_ewma : 0;
                }

            }

        }


        //write events
        m_event_buffer.push_batch(&bach_to_client[0], cnt_event_to_client);
        cnt_event_to_client = 0;


        if (to_process > 0) {
            m_hot_buffer.update_tail(reader_idx);
            empty_cycles = 0;
            //_mm_pause(); // No!
        }
        else {
            empty_cycles++;
            if (empty_cycles < 1000) {
                _mm_pause();
            }
            else if (empty_cycles < 50000) {
                for (int j = 0; j < 10; ++j)
                    _mm_pause();
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

/////////////////////////////////////////////////////


void Server::event_dispatcher()
{
    SetThreadAffinityMask(GetCurrentThread(), 1 << 4);

    std::vector<Session*> clients;

    uint64_t iter_count = 0;

    uint64_t total_dropped = 0;

    int empty_cycles = 0;

    uint64_t reader_idx = m_event_buffer.get_head();
    uint64_t last_tail_update = reader_idx;
    while (m_running)
    {
        uint64_t h = m_event_buffer.get_head();

        uint64_t avail_read = h - reader_idx;

        //////
        //if (avail_read > m_event_buffer.capacity() * 0.9) {
        //    reader_idx = h;
        //    m_event_buffer.update_tail(reader_idx);
        //    printf("\nevent_dispatcher OVERLOADED! DROPS!\n");
        //}
        //////

        if (h <= reader_idx) {
            _mm_pause();
            continue;
        }

        //// update local_clients 
        if (m_need_update_clients.load(std::memory_order_acquire))
        {
            std::lock_guard<std::mutex> lock(m_mtx_subscribers);
            m_need_update_clients.store(false, std::memory_order_relaxed);
            clients.clear();
            clients.reserve(m_subscribers.size());

            for (auto& sp : m_subscribers) {
                {
                    clients.push_back(sp.get());
                }
            }
        }

        size_t to_process = (avail_read < 1024) ? avail_read : 1024;

        for (size_t i = 0; i < to_process; ++i)
        {
            const auto& ev = m_event_buffer.read(reader_idx++);

            for (auto& pSession : clients)
            {
                if (ev.index_symbol == pSession->GetSymbolIndex() && ev.total_usd() >= pSession->GetWhaleTreshold())
                    pSession->PushEvent(ev);
            }
 
        }

        if (reader_idx - last_tail_update >= 512/*1024*/) 
        {
            m_event_buffer.update_tail(reader_idx);
            last_tail_update = reader_idx;
        }

        if (to_process > 0) {
            empty_cycles = 0;
            //_mm_pause();
        }
        else {
            empty_cycles++;
            if (empty_cycles < 1000) {
                _mm_pause();
            }
            else if (empty_cycles < 50000) {
                for (int j = 0; j < 10; ++j) 
                    _mm_pause();
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
