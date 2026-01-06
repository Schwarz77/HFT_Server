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

    m_session_dispatcher = std::thread(&Server::session_dispatcher, this);
    //m_producer = std::thread(&Server::producer_loop, this);
    m_hot_dispatcher = std::thread(&Server::hot_dispatcher, this);
    m_event_dispatcher = std::thread(&Server::event_dispatcher, this);
    m_monitor = std::thread(&Server::speed_monitor, this);
    m_producer = std::thread(&Server::producer_loop, this);

    set_affinity(m_producer, 0);
    //set_affinity(m_session_dispatcher, 2);
    set_affinity(m_hot_dispatcher, 2);
    set_affinity(m_event_dispatcher, 4);
    set_affinity(m_monitor, 5);  // tail to 3 core

}

Server::~Server() 
{
    Stop();
}

void Server::Start() 
{
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

    // wake dispatcher
    m_cv_queue.notify_all();

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

    // just in case - for guaranteed absence of leaks
    clear_sessions(); 
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

}

//void Server::SetSignals(const VecSignal signals)
//{
//    asio::post(m_io, [this, signals]()
//        {
//            // Closing all client connections so that clients can reconnect and receive the changed count of signals.
//
//            std::lock_guard<std::mutex> lk(m_mtx_subscribers);
//
//            for (auto it = m_subscribers.begin(); it != m_subscribers.end();)
//            {
//                if (auto sp = it->lock())
//                {
//                    sp->ForceClose();
//                    ++it;
//                }
//                else
//                {
//                    it = m_subscribers.erase(it);
//                }
//            }
//
//            // set signals
//
//            {
//                std::lock_guard<std::mutex> lk(m_mtx_state);
//
//                m_state.clear();
//
//                auto now = steady_clock::now();
//
//                for (auto s : signals)
//                {
//                    m_state[s.id] = s;
//                }
//            }
//        });
//}

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

//bool Server::PushSignal(const Signal& s) 
//{
//    bool pushed = false;
//
//    {
//        std::lock_guard<std::mutex> lk_state(m_mtx_state);
//
//        auto it = m_state.find(s.id);
//
//        if (it != m_state.end() && s.ts >= it->second.ts)
//        {
//            m_state[s.id] = s;
//            pushed = true;
//        }
//    }
//
//    if (pushed)
//    {
//        {
//            std::lock_guard<std::mutex> lk_queue(m_mtx_queue);
//            m_queue.push_back(s);
//        }
//        m_cv_queue.notify_one();
//    }
//
//    return pushed;
//}
//
//bool Server::GetSignal(int id, Signal& s)
//{
//    std::lock_guard<std::mutex> lk(m_mtx_state);
//
//    auto it = m_state.find(id);
//    if (it != m_state.end())
//    {
//        s = it->second;
//        return true;
//    }
//
//    return false;
//}
//
//VecSignal Server::GetSnapshot(uint8_t type) 
//{
//    VecSignal out;
//    std::lock_guard<std::mutex> lk(m_mtx_state);
//
//    for (auto& p : m_state)
//    {
//        if ((uint8_t)p.second.type & type)
//        {
//            out.push_back(p.second);
//        }
//    }
//
//    return out;
//}

void Server::session_dispatcher() 
{
    while (m_running) 
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        UnregisterExpired();

        ////VecSignal batch;

        ////{
        ////    std::unique_lock<std::mutex> lk(m_mtx_queue);

        ////    m_cv_queue.wait(lk, [&] 
        ////        {
        ////            return !m_queue.empty() || !m_running; 
        ////        });

        ////    while (!m_queue.empty()) 
        ////    {
        ////        batch.push_back(m_queue.front()); 
        ////        m_queue.pop_front(); 
        ////    }
        ////}

        ////if (!batch.empty()) 
        //{
        //    // delivery: broadcast to subscribers
        //    std::lock_guard<std::mutex> lk(m_mtx_subscribers);

        //    for (auto it = m_subscribers.begin(); it != m_subscribers.end();) 
        //    {
        //        if ((*it)->Expired())
        //        {
        //            //sp->DeliverUpdates(batch);
        //            ++it;
        //        }
        //        else
        //        {
        //            it = m_subscribers.erase(it);
        //        }
        //    }
        //}

        //m_need_update_clients.store(true, std::memory_order_release);

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
        m_mapCoinSymbol[i] = coin_data[i].symbol;
    }
}

std::string Server::GetCoinSymbol(int index)
{
    std::lock_guard<std::mutex> lk_symb(m_mtx_coin_symbol);

    auto it = m_mapCoinSymbol.find(index);
    if (it != m_mapCoinSymbol.end())
        return it->second;

    return std::string();
}

void Server::producer_loop()
{
    SetThreadAffinityMask(GetCurrentThread(), 1 << 0);

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
        double m_eps;
        if (speed >= 1'000'000.0)
        {
            m_eps = speed / 1'000'000.0; // Million per sec

            std::cout << "\r"
                << "Throughput: " << std::fixed << std::setprecision(2) << m_eps << " Million event/sec | "
                << "Total: " << current_head / 1'000'000 << "M events"
                << std::flush;
        }
        else if (speed >= 1'000.0)
        {
            m_eps = speed / 1'000.0; // Thousand per sec

            std::cout << "\r" 
                << "Throughput: " << std::fixed << std::setprecision(2) << m_eps << " Thousand event/sec | "
                << "Total: " << current_head / 1'000 << "K events"
                << std::flush;
        }
        else
        {
            m_eps = speed; 

            std::cout << "\r"
                << "Throughput: " << std::fixed << std::setprecision(2) << m_eps << " event/sec | "
                << "Total: " << current_head << "events"
                << std::flush;
        }

        //auto lag = m_hot_buffer.get_head() - last_reader_idx;
        //double lag_percent = (static_cast<double>(lag) / global_rb.capacity()) * 100.0;

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

inline void Server::parse_single_event(simdjson::dom::element item) {
    MarketEvent event{};

    // 's' - it's deal
    std::string_view s;
    if (item["s"].get(s) == simdjson::error_code::SUCCESS) {
        // Timestamp
        event.timestamp = item["E"].get_uint64();

        // Symbol
        size_t len = std::min<size_t>(s.size(), sizeof(event.symbol) - 1);
        std::memcpy(event.symbol, s.data(), len);
        event.symbol[len] = '\0';

        // Price & Quantity (строки в JSON -> double)
        std::string_view p_str = item["p"].get_string();
        std::from_chars(p_str.data(), p_str.data() + p_str.size(), event.price);

        std::string_view q_str = item["q"].get_string();
        std::from_chars(q_str.data(), q_str.data() + q_str.size(), event.quantity);

        // Side
        event.is_sell = item["m"].get_bool();

        if (event.timestamp > 0) {
            //m_hot_buffer.push(event);
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

void Server::binance_stream() {
    ix::initNetSystem();

    static ix::WebSocket webSocket;
    //std::string url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
    //std::string url = "wss://stream.binance.com:9443/ws/!ticker@arr";
    //std::string url = "wss://stream.binance.com:9443/ws/!aggTrade@arr";
    //std::string url = "wss://stream.binance.com:9443/stream?streams=!aggTrade@arr";
    //std::string url = "wss://stream.binance.com:9443/stream?streams=%21aggTrade@arr";

    std::string url = "wss://fstream.binance.com/ws";


    webSocket.setUrl(url);

    webSocket.enablePerMessageDeflate();

    static thread_local simdjson::ondemand::parser parser;

    static std::atomic<int> cnt = 0, cnt2 = 0;

    webSocket.setOnMessageCallback([&](const ix::WebSocketMessagePtr& msg) {
        if (msg->type == ix::WebSocketMessageType::Open) {
            std::cout << "[Main] Connection opened!" << std::endl;
            //     webSocket.send(R"({"method":"SUBSCRIBE","params":["!aggTrade@arr"],"id":1})");
                 //webSocket.send(R"({"method":"SUBSCRIBE","params":["!aggTrade@arr","btcusdt@aggTrade"],"id":1})");
             //    std::string sub = R"({
             //    "method": "SUBSCRIBE",
             //    "params": [
             //        "btcusdt@aggTrade", "ethusdt@aggTrade", "solusdt@aggTrade", 
             //        "dogeusdt@aggTrade", "xrpusdt@aggTrade", "adausdt@aggTrade",
             //        "linkusdt@aggTrade", "trxusdt@aggTrade", "dotusdt@aggTrade",
             //        "nearusdt@aggTrade", "pepeusdt@aggTrade", "bnbusdt@aggTrade"
             //    ],
             //    "id": 1
             //})";

             //    std::string sub = R"({
             //    "method": "SUBSCRIBE",
             //    "params": [
             //                "btcusdt@trade",
             //                "ethusdt@trade",
             //                "bnbusdt@trade",
             //                "btcusdt@depth",
             //                "ethusdt@depth",
             //                "bnbusdt@depth"
             //    ],
             //    "id": 1
             //})";

            std::string sub = R"({"method": "SUBSCRIBE", "params": ["!bookTicker"], "id": 1})";

            webSocket.send(sub);
        }
        else if (msg->type == ix::WebSocketMessageType::Message) {
            process_market_msg(msg);

            if (cnt - cnt2 > 100)
            {
                std::cout << "\ncnt: " << cnt.load() << std::endl;
                //printf("Cnt: %d\n", cnt.load());
                cnt2.store(cnt.load(), std::memory_order_relaxed);
            }
            cnt++;

        }
        //else if (msg->type == ix::WebSocketMessageType::Error) {
        //    std::cerr << std::endl << "Error: " << msg->errorInfo.reason << std::endl;
        //}
        });

    webSocket.setPingInterval(30);

    webSocket.start();
}


void Server::hot_dispatcher() 
{
    SetThreadAffinityMask(GetCurrentThread(), 1 << 2);

    uint64_t reader_idx = m_hot_buffer.get_head();
    const int threshold_test = 5000;

    int empty_cycles = 0;

    uint64_t iter_count = 0;

    uint64_t total_dropped = 0;

    //std::memset(coin_VWAP, 0, sizeof(CoinVWAP) * COIN_CNT);

    //double b_pv[COIN_CNT];
    //double b_v[COIN_CNT];
    //SecAgg  sec_aggs[COIN_CNT][MAX_SEC_IN_BATCH];
    //uint8_t sec_cnt[COIN_CNT];


    std::vector<WhaleEvent> bach_to_client(1024);
    uint64_t cnt_event_to_client = 0;

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
    
        //std::memset(b_pv, 0, 8 * COIN_CNT);
        //std::memset(b_v, 0, 8 * COIN_CNT);
        //std::memset(sec_aggs, 0, sizeof(SecAgg) * COIN_CNT * MAX_SEC_IN_BATCH);
        //std::memset(sec_cnt, 0, sizeof(uint8_t) * COIN_CNT);

        uint64_t first_sec = 0;
        uint64_t last_sec = 0;
        bool first = true;

        for (size_t i = 0; i < to_process; ++i) {
            const auto& ev = m_hot_buffer.read(reader_idx++);

            if (ev.index_symbol < 0  || ev.index_symbol >= COIN_CNT)
                continue;

            double pv = ev.total_usd();
            //b_pv[ev.index_symbol] += pv;
            //b_v[ev.index_symbol] += ev.quantity;

            uint64_t sec = ev.timestamp / 1000;

            //if(i == 0)
            //    first_sec = sec;
            //last_sec = sec;

            auto& c = coin_VWAP[ev.index_symbol];

            c.session.add(ev.price, ev.quantity);
            c.roll50.add(ev.price, ev.quantity);
            c.ewma.add(ev.price, 0.05);

            if (pv >= whale_treshold[ev.index_symbol] ) [[unlikely]]
            {
                WhaleEvent& we = bach_to_client[cnt_event_to_client++];
                we.index_symbol = ev.index_symbol;
                we.price = ev.price;
                we.quantity = ev.quantity;
                we.timestamp = ev.timestamp;
                //we.is_sell = ev.is_sell;  //?
                we.vwap_sess = c.session.value();
                we.vwap_roll50 = c.roll50.value();
                we.vwap_ewma = c.ewma.value();

                we.delta_roll = ev.price - we.vwap_roll50;
                we.delta_ewma = ev.price - we.vwap_ewma;

                //we.vwap_1m = vwap_1m(coin_VWAP[ev.index_symbol]);
                //we.vwap_1d = vwap_1d(coin_VWAP[ev.index_symbol]);


            }

            //// slow path accumulation
            //uint8_t& cnt = sec_cnt[ev.index_symbol];
            //if (cnt == 0 || sec_aggs[ev.index_symbol][cnt - 1].sec != sec) {
            //    if (cnt < MAX_SEC_IN_BATCH) {
            //        sec_aggs[ev.index_symbol][cnt++] = { sec, 0.0, 0.0 };
            //    }
            //    else
            //    {
            //        // overflow
            //        int ddd = 0;
            //     }
            //}
        }

        //if (first_sec == last_sec) [[likely]]
        //{
        //    for (int s = 0; s < COIN_CNT; ++s)
        //    {
        //        if (b_v[s] > 0.0) {
        //            apply_batch(coin_VWAP[s], b_pv[s], b_v[s], first_sec);
        //        }
        //    }
        //}
        //else [[unlikely]]
        //{
        //    for (int s = 0; s < COIN_CNT; ++s)
        //    {
        //        for (uint8_t i = 0; i < sec_cnt[s]; ++i) {
        //            apply_batch(
        //                coin_VWAP[s],
        //                sec_aggs[s][i].pv,
        //                sec_aggs[s][i].v,
        //                sec_aggs[s][i].sec
        //            );
        //        }
        //    }
        //}

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
                if (ev.index_symbol == pSession->m_ind_symb && ev.total_usd() >= pSession->GetWhaleTreshold())
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
