// server.cpp

#include "Server.h"
#include "Session.h"
#include <iostream>
#include <chrono>
#include <Utils.h>


namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;
using time_point = std::chrono::steady_clock::time_point;
using steady_clock = std::chrono::steady_clock;


void set_affinity(std::thread& t, int logical_core_id) 
{
    HANDLE handle = t.native_handle();
    // Маска: 1 << id. Для 0 это 1, для 2 это 4.
    DWORD_PTR mask = (1ULL << logical_core_id);
    if (SetThreadAffinityMask(handle, mask) == 0) 
    {
        std::cerr << "Error setting affinity\n";
    }
}

Server::Server(asio::io_context& io, uint16_t port)
    : m_io(io), m_acceptor(io, tcp::endpoint(tcp::v4(), port), true/*false*/)
{
    //Start();

    m_session_dispatcher = std::thread(&Server::session_dispatcher, this);
    //m_producer = std::thread(&Server::producer_loop, this);
    m_hot_dispatcher = std::thread(&Server::hot_dispatcher, this);
    m_event_dispatcher = std::thread(&Server::event_dispatcher, this);
    m_monitor = std::thread(&Server::speed_monitor, this);
    m_producer = std::thread(&Server::producer_loop, this);

    set_affinity(m_producer, 0);
    //set_affinity(m_session_dispatcher, 2);
    set_affinity(m_hot_dispatcher, 2);
    set_affinity(m_monitor, 3);  // tail to 2 core
    set_affinity(m_event_dispatcher, 4);

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
                    std::cout << "Accepted connection\n";

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
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

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
    //{
        std::lock_guard<std::mutex> lk(m_mtx_subscribers);

        m_subscribers.push_back(s);
    //}

    //sync_snapshot();

}

void Server::UnregisterExpired() 
{
    //{
        std::lock_guard<std::mutex> lk(m_mtx_subscribers);

        m_subscribers.erase(std::remove_if(m_subscribers.begin(), m_subscribers.end(),
            [](const std::weak_ptr<Session>& w)
            {
                return w.expired();
            }),
            m_subscribers.end());
    //}

    //sync_snapshot();

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
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        

        //VecSignal batch;

        //{
        //    std::unique_lock<std::mutex> lk(m_mtx_queue);

        //    m_cv_queue.wait(lk, [&] 
        //        {
        //            return !m_queue.empty() || !m_running; 
        //        });

        //    while (!m_queue.empty()) 
        //    {
        //        batch.push_back(m_queue.front()); 
        //        m_queue.pop_front(); 
        //    }
        //}

        //if (!batch.empty()) 
        //{
            // delivery: broadcast to subscribers
            std::lock_guard<std::mutex> lk(m_mtx_subscribers);

            for (auto it = m_subscribers.begin(); it != m_subscribers.end();) 
            {
                if (auto sp = it->lock()) 
                {
                    //sp->DeliverUpdates(batch);
                    ++it;
                }
                else
                {
                    it = m_subscribers.erase(it);
                }
            }
        //}

        //sync_snapshot();
    }
}

///////////////////////////////////
//
        // Предварительно создаем шаблоны для пар (ускоряем генерацию)
struct Template {
    char symbol[16];
    double price;
};


const Template templates[] = {
    {"BTCUSDT", 96000.0}, {"ETHUSDT", 2700.0}, {"SOLUSDT", 180.0}, {"BNBUSDT", 600.0}
};

//struct alignas(64) CoinAnalytics {
//    // Дневной VWAP
//    double day_pv = 0.0;
//    double day_v = 0.0;
//
//    // Минутное окно (60 секундных бакетов)
//    struct Bucket { double pv = 0.0; double v = 0.0; };
//    Bucket buckets[60] = {};
//    double rolling_pv = 0.0;
//    double rolling_v = 0.0;
//
//    uint64_t last_sec = 0;
//    uint8_t cursor = 0;
//
//    void apply_batch(double batch_pv, double batch_q, uint64_t ts_sec) {
//        day_pv += batch_pv;
//        day_v += batch_q;
//
//        if (ts_sec > last_sec) {
//            uint64_t diff = std::min<uint64_t>(ts_sec - last_sec, 60);
//
//            for (uint64_t i = 0; i < diff; ++i) {
//                cursor++;
//                if (cursor >= 60) 
//                    cursor = 0; // fast increment instead of %
//
//                // subtract old data that falls outside the one-minute window.
//                rolling_pv -= buckets[cursor].pv;
//                rolling_v -= buckets[cursor].v;
//
//                // reset the bucket since it's a new second.
//                buckets[cursor] = { 0.0, 0.0 };
//            }
//            last_sec = ts_sec;
//        }
//
//        // Add the accumulated amount for the pack to the current (cleared) bucket
//        buckets[cursor].pv += batch_pv;
//        buckets[cursor].v += batch_q;
//        rolling_pv += batch_pv;
//        rolling_v += batch_q;
//    }
//};

struct CoinVWAP {
    // 1d
    double day_pv = 0.0;
    double day_v = 0.0;

    // 1m sliding
    struct Bucket { double pv, v; };
    Bucket buckets[60] = {};
    double min_pv = 0.0;
    double min_v = 0.0;

    uint64_t last_sec = 0;
    uint8_t  cursor = 0;
};

inline uint32_t hash_symbol(const char* str)
{
    uint32_t hash = 0x811c9dc5;
    while (*str) {
        hash ^= (uint8_t)*str++;
        hash *= 0x01000193;
    }
    return hash;
}

double whale_treshold[_countof(templates)] = {100000, 70000, 50000, 60000};

//CoinAnalytics ar_coin_stat[_countof(templates)];

CoinVWAP coin_VWAP[_countof(templates)];

double b_day_pv[_countof(templates)] = { 0 };
double b_day_v[_countof(templates)] = { 0 };

inline void process_VWAP(CoinVWAP& c,
    double price,
    double qty,
    uint64_t ts_sec)
{
    double pv = price * qty;

    // === DAY ===
    c.day_pv += pv;
    c.day_v += qty;

    // === 1 MIN SLIDING ===
    if (ts_sec != c.last_sec) {
        uint64_t diff = ts_sec - c.last_sec;
        if (diff > 60) diff = 60;

        for (uint64_t i = 0; i < diff; ++i) {
            c.cursor = (c.cursor + 1 == 60) ? 0 : c.cursor + 1;
            c.min_pv -= c.buckets[c.cursor].pv;
            c.min_v -= c.buckets[c.cursor].v;
            c.buckets[c.cursor] = { 0.0, 0.0 };
        }
        c.last_sec = ts_sec;
    }

    c.buckets[c.cursor].pv += pv;
    c.buckets[c.cursor].v += qty;
    c.min_pv += pv;
    c.min_v += qty;
}

inline double vwap_1m(const CoinVWAP& c) {
    return c.min_v > 0 ? c.min_pv / c.min_v : 0.0;
}

inline double vwap_1d(const CoinVWAP& c) {
    return c.day_v > 0 ? c.day_pv / c.day_v : 0.0;
}

//
//////////////////////////////////////

void Server::producer_loop()
{
    SetThreadAffinityMask(GetCurrentThread(), 1 << 0);

    for (int i = 0; i < _countof(templates); i++)
    {
        m_reg_coin.register_coin(templates[i].symbol, i);
    }

    const size_t size_batch = 64; // optimal size for L1 cash 
    std::vector<MarketEvent> batch(size_batch);

    //std::mt19937_64 rng(std::random_device{}());

    int64_t cnt = 0;
    int64_t cnt_whale = 0;

    uint64_t batch_ts = std::chrono::duration_cast<std::chrono::milliseconds>( // сделать для ветки или больше
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    uint64_t m_dropped_producer = 0;


    int cnt_templ = _countof(templates);

    while (m_running) {

        if (m_hot_buffer.can_write(size_batch))
        {
            for (int i = 0; i < size_batch; ++i) {
                int ind = i% cnt_templ;
                auto& t = templates[ind];

                if (cnt % 50000000 == 0)
                //if(cnt++ >= 50000000)
                {
                    batch_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()
                    ).count();
                }

                //if (cached_type) [[likely]]
                {
                    MarketEvent& ev = batch[i];

                    //int sz = sizeof(ev);

                    ev.timestamp = batch_ts;
                    std::memcpy(ev.symbol, t.symbol, 16);
                    ev.price = t.price + (i % 10) * 0.1;
                    ev.quantity = (cnt % 25000000 == 32) ? 100 : 1.0;
                    ev.is_sell = (i % 2 == 0);
                    //uint64_t s1 = *(reinterpret_cast<const uint64_t*>(t.symbol));
                    //ev.index_symbol = m_reg_coin.get_index_fast(s1);
                    ////ev.symbol_hash = CoinRegistry::fast_hash(t.symbol); //hash_symbol(t.symbol);
                    ev.index_symbol = ind;

                    cnt++;
                    if (ev.quantity > 99 && ev.total_usd() >= 960000.5 /*&& ev.index_symbol == 0*/ /*hash_symbol(ev.symbol) == hash_symb*/ /*ev.index_symbol == 0*/ )
                    {
                        cnt_whale++;

                        //if (cnt_whale % 10 == 0)
                        //    std::cout << std::endl << "W:" << cnt_whale << std::endl;
                    }
                }

            }

            //m_hot_buffer.push_batch(batch);
            m_hot_buffer.push_batch(batch.data(), batch.size());
        }
        else
        {
            m_dropped_producer += size_batch;
        }

        _mm_pause(); // tmp dbg
        //for (int j = 0; j < 10; ++j)
        //    std::this_thread::yield();
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
    //{
        std::lock_guard<std::mutex> lk(m_mtx_subscribers);

        for (auto& it : m_subscribers)
        {
            if (auto sp = it.lock())
            {
                sp->ForceClose();
            }
        }

        m_subscribers.clear();
   // }

    //sync_snapshot();

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
                std::cout << "cnt: " << cnt.load() << std::endl;
                //printf("Cnt: %d\n", cnt.load());
                cnt2.store(cnt.load(), std::memory_order_relaxed);
            }
            cnt++;

        }
        //else if (msg->type == ix::WebSocketMessageType::Error) {
        //    std::cerr << "Error: " << msg->errorInfo.reason << std::endl;
        //}
        });

    webSocket.setPingInterval(30);

    webSocket.start();
}


void Server::hot_dispatcher() {
    SetThreadAffinityMask(GetCurrentThread(), 1 << 2);

    uint64_t reader_idx = m_hot_buffer.get_head();
    const int threshold_test = 5000;

    int empty_cycles = 0;

    uint64_t iter_count = 0;

    uint64_t total_dropped = 0;

    while (m_running) {
        uint64_t h = m_hot_buffer.get_head();

        //
        if (h - reader_idx > BUFFER_SIZE * 0.9) {
            // We are falling behind, it's drops
            reader_idx = h;
            m_hot_buffer.update_tail(reader_idx);
            printf("\nhot_dispatcher OVERLOADED! JUMPING TO HEAD\n");

        }
        //

        if (h <= reader_idx) {
            _mm_pause();
            continue;
        }

        // Параметры пачки
        size_t to_process = std::min<size_t>(h - reader_idx, 1024);
      
        for (size_t i = 0; i < to_process; ++i) {
            const auto& ev = m_hot_buffer.read(reader_idx++);

            if (ev.index_symbol < 0)
                continue;

            double pv = ev.total_usd();
            b_day_pv[ev.index_symbol] += pv;
            b_day_v[ev.index_symbol] += ev.quantity;

            //process_VWAP(coin_VWAP[ev.index_symbol], ev.price, ev.quantity, ev.timestamp / 1000);

            if (pv >= whale_treshold[ev.index_symbol])
            {
                WhaleEvent we;
                we.index_symbol = ev.index_symbol;
                we.total_usd = pv;
                we.timestamp = ev.timestamp;
                //we.vwap_1m = vwap_1m(coin_VWAP[ev.index_symbol]);
                //we.vwap_1d = vwap_1d(coin_VWAP[ev.index_symbol]);

                m_event_buffer.push_batch(&we, 1);
            }

        }

        //if(to_process > 0)
        //    m_hot_buffer.update_tail(reader_idx);

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

    std::vector<std::shared_ptr<Session>> local_clients;
    std::vector<Session*> local_clients_2;

    uint64_t iter_count = 0;

    uint64_t total_dropped = 0;

    auto last_client_update = std::chrono::steady_clock::now();
    auto client_list_update = std::chrono::milliseconds(100);

    //1. Обновляем клиентов (вынесено в начало для чистоты)
    //auto now = std::chrono::steady_clock::now();
    //if (now - last_client_update > client_list_update)
    //{
    //    std::lock_guard<std::mutex> lock(m_mtx_subscribers);
    //    local_clients.clear();
    //    local_clients_2.clear();

    //    int id_cli = 0;
    //    for (auto& wp : m_subscribers) {
    //        if (auto sp = wp.lock())
    //        {
    //            local_clients.push_back(sp);
    //            local_clients_2.push_back(sp.get());
    //            id_cli++;
    //        }
    //    }
    //    //last_client_update = now;
    //}


    int empty_cycles = 0;

    uint64_t reader_idx = m_event_buffer.get_head();
    uint64_t last_tail_update = reader_idx;
    while (m_running)
    {
        ////
        // 1. Читаем голову ОДИН раз
        uint64_t h = m_event_buffer.get_head(); // memory_order_acquire

        //
        if (h - reader_idx > COLD_BUFFER_SIZE * 0.9) {
            reader_idx = h;
            m_event_buffer.update_tail(reader_idx);
            printf("\nevent_dispatcher OVERLOADED! JUMPING TO HEAD\n");
        }
        //

        if (h <= reader_idx) {
            _mm_pause();
            continue;
        }

        //// update local_clients 
        //auto now = std::chrono::steady_clock::now();
        //if (now - last_client_update > client_list_update) 
        if(local_clients_2.empty())
        {
            std::lock_guard<std::mutex> lock(m_mtx_subscribers);
            local_clients.clear();
            local_clients_2.clear();

            int id_cli = 0;
            for (auto& wp : m_subscribers) {
                if (auto sp = wp.lock())
                {
                    local_clients.push_back(sp);
                    local_clients_2.push_back(sp.get());
                    id_cli++;
                }
            }
            //last_client_update = now;
        }

        //if (need_update_clients.load(std::memory_order_acquire)) {
        //    need_update_clients.store(false, std::memory_order_relaxed);

        //    std::lock_guard<std::mutex> lock(m_mtx_subscribers);
        //    local_clients.clear();
        //    local_clients_2.clear();

        //    for (auto& wp : m_subscribers) {
        //        if (auto sp = wp.lock()) {
        //            local_clients.push_back(sp);
        //            local_clients_2.push_back(sp.get());
        //        }
        //    }
        //}

        size_t to_process = std::min<size_t>(h - reader_idx, 1024);

        //auto snap = std::atomic_load(&m_current_snapshot, );

        for (size_t i = 0; i < to_process; ++i)
        {
            const auto& ev = m_event_buffer.read(reader_idx++);

            for (auto& pSession : local_clients_2)
            //for (Session* pSession : snap->ptrs)
            {
                if (ev.index_symbol == pSession->m_ind_symb && ev.total_usd >= pSession->GetWhaleTreshold())
                    pSession->PushEvent(ev);
            }
 
        }

        // Без этого market_dispatcher упадет в 0 через секунду!
        if (reader_idx - last_tail_update >= 512/*1024*/) {
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

//void Server::client_update_timer() 
//{
//    auto client_list_update = std::chrono::milliseconds(100);
//
//    while (m_running) 
//    {
//        std::this_thread::sleep_for(client_list_update);
//        need_update_clients.store(true, std::memory_order_release);
//    }
//}
//
//void Server::sync_snapshot() 
//{
//    auto new_snap = std::make_shared<SessionSnapshot>();
//
//    {
//        std::lock_guard<std::mutex> lk(m_mtx_subscribers);
//
//        for (auto it = m_subscribers.begin(); it != m_subscribers.end(); ) {
//            if (auto sp = it->lock()) {
//                new_snap->ptrs.push_back(sp.get());
//                new_snap->strong.push_back(std::move(sp));
//                ++it;
//            }
//            else {
//                it = m_subscribers.erase(it);
//            }
//        }
//    }
//    std::atomic_store(&m_current_snapshot, new_snap);
//}