// server.cpp

#include "Server.h"
#include "Session.h"
#include <iostream>
#include <chrono>
#include <Utils.h>

#ifndef _WIN32
#include <sys/resource.h>
#endif

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;
using time_point = std::chrono::steady_clock::time_point;
using steady_clock = std::chrono::steady_clock;


///////////////////////////////////////////////////////////////////////
//
// 
        // it's global data. if move it to class member - speed will decrease slightly 

// const array - max speed
const CoinPair coins[] =
{
    {"BTCUSDT", 96000.0}, {"ETHUSDT", 2700.0}, {"SOLUSDT", 180.0}, {"BNBUSDT", 600.0}
};

const size_t COIN_CNT = std::size(coins);

double whale_global_treshold[COIN_CNT] = { 100000, 70000, 50000, 60000 };

CoinAnalytics coin_VWAP[COIN_CNT];



//// vector - for init data from .ini
//const size_t COIN_CNT_GEN_DBG = 4; // coins count (will be created)
////const size_t COIN_CNT_GEN_DBG = 1024;
//size_t COIN_CNT = 0;
//std::vector<CoinPair> coins;
//std::vector<double> whale_global_treshold;
//std::vector<CoinAnalytics> coin_VWAP;
////


//
//
///////////////////////////////////////////////////////////////////////


#ifdef _WIN32
void set_affinity(std::thread& t, int logical_core_id)
{

    HANDLE handle = t.native_handle();

    DWORD_PTR mask = (1ULL << logical_core_id);
    if (SetThreadAffinityMask(handle, mask) == 0)
    {
        std::cerr << "\nError setting affinity\n"; 
    }
}
#else
void set_affinity(pthread_t t, int logical_core_id)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(logical_core_id, &cpuset);
    int result = pthread_setaffinity_np(t, sizeof(cpu_set_t), &cpuset);
    //int result = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (result != 0) {
        perror("\nError pthread_setaffinity_np\n");
    }
}
#endif

struct LatencySnapshot 
{
    std::atomic<uint64_t> total_ticks;
    std::atomic<uint64_t> count;

    static const size_t BUCKET_SHIFT = 10; // 2^10 = 1024 tics per backet
    uint64_t buckets[4096] = { 0 };
    uint64_t buckets_snapshot[4096] = { 0 };
    std::atomic<bool> snapshot_ready{ false };
};


LatencySnapshot stat_latency;

inline uint64_t rdtsc() 
{
    return __rdtsc();
}

void Server::set_cpu_ghz()
{
    auto t1 = std::chrono::steady_clock::now();
    uint64_t r1 = __rdtsc();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto t2 = std::chrono::steady_clock::now();
    uint64_t r2 = __rdtsc();

    auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();

    //std::cout << "\ntsc_ghz " << (double)(r2 - r1) / (double)duration_ns << std::endl;

    m_cpu_ghz = (double)(r2 - r1) / (double)duration_ns;
}

Server::Server(asio::io_context& io, uint16_t port)
    : m_io(io), m_acceptor(io, tcp::endpoint(tcp::v4(), port), true/*false*/)
{
    init_coin_data();
    register_coins();
    set_cpu_ghz();
}

Server::~Server() 
{
    Stop();
}

void Server::Start() 
{

#ifdef _WIN32
    SetPriorityClass(GetCurrentProcess(), HIGH_PRIORITY_CLASS);
#endif


    m_session_dispatcher = std::thread(&Server::session_dispatcher, this);
    m_hot_dispatcher = std::thread(&Server::hot_dispatcher, this);
    m_event_dispatcher = std::thread(&Server::event_dispatcher, this);
    m_monitor = std::thread(&Server::speed_monitor, this);
    m_producer = std::thread(&Server::producer, this);

   
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

            }
        });
}

void Server::Stop() 
{
    m_running = false;
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

        size_t removed_count = std::erase_if(m_subscribers, [](const auto& s) {
            return s->Expired();
            });

        if (removed_count > 0) {
            is_changed = true;
        }
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


void Server::init_coin_data()
{
    // do it once ! 
    // COIN_CNT don't change after it!

    std::call_once(m_coins_initialized, [this]() 
        {
            // TODO: init data from .ini
            // ...

            //////////////////////////////////////////////////////////
            ///// gen by count COIN_CNT_GEN_DBG // speed 130M at 4 coin & vwap_session, 
            //std::vector<CoinPair> vecCoins{ { "BTCUSDT", 96000.0 }, { "ETHUSDT", 2700.0 }, { "SOLUSDT", 180.0 }, { "BNBUSDT", 600.0 } };
            //for (int i = 0; i < COIN_CNT_GEN_DBG - vecCoins.size(); i++)
            //{
            //    char name[16] = "";
            //    sprintf(name, "B%d", i);
            //    CoinPair cp;
            //    strcpy(cp.symbol, name);
            //    cp.price = 10000;
            //    vecCoins.push_back(cp);

            //}
            //COIN_CNT = vecCoins.size();

            //coins.reserve(COIN_CNT);
            //for(int i=0; i< COIN_CNT; i++)
            //    coins.push_back(vecCoins[i]);
            //coins.shrink_to_fit(); // don't change size after it!
            //
            // 
            //whale_global_treshold.reserve(COIN_CNT);
            ////for (auto d : std::vector<double>{ 100000, 70000, 50000, 60000 })
            ////    whale_global_treshold.push_back(d);
            //for (int i = 0; i < COIN_CNT; i++)
            //    whale_global_treshold.push_back(100'000);
            //whale_global_treshold.shrink_to_fit();

            //////////////////////////////////////////////////////////

        });
}

void Server::register_coins()
{
    for (int i = 0; i < COIN_CNT; i++)
    {
        m_reg_coin.register_coin(coins[i].symbol, i);
    }

}

std::string Server::GetCoinSymbol(int index) const
{
    if (index >= 0 && index < COIN_CNT)
        return coins[index].symbol;

    return std::string();
}

int Server::GetCoinIndex(std::string& symbol) const
{
    return m_reg_coin.get_index_coin(symbol.data());
}

void Server::producer()
{

#ifdef _WIN32
    set_affinity(m_producer, 0);
    SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
#else
    set_affinity(pthread_self(), 2);
    setpriority(PRIO_PROCESS, 0, -20);
#endif

    if (m_data_emulation.load(std::memory_order_acquire))
    {
        emulator_loop();
    }
    else
    {
        binance_stream();

    }
}

void Server::emulator_loop()
{
    const size_t size_batch = 64;

    int64_t cnt = 0;

    int64_t cnt_whale_gen = 0;
    int64_t cnt_tm_upd = 0;

    uint64_t batch_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    uint64_t m_dropped_producer = 0;

    // for generation whale qty 
    std::vector<uint32_t> qty;
    std::vector<uint32_t> qty_rnd;
    qty.reserve(COIN_CNT);
    qty_rnd.reserve(COIN_CNT);
    for (int i = 0; i < COIN_CNT; i++)
    {
        qty.push_back((int)whale_global_treshold[i] / coins[i].price);
        qty_rnd.push_back(qty[i] * 5);
    }

    m_need_reset_vwap.store(true, std::memory_order_release);

    const uint64_t overload_val = m_hot_buffer.capacity() * 0.7;
    uint32_t check_counter = 0;
    bool overloaded = false;

    while (m_running)
    {
        if ((check_counter++ & 127) == 0) 
        {
            if (m_hot_buffer.get_used_size() > overload_val)
            {
                _mm_pause();
                continue;
            }
        }

        size_t cnt_write = size_batch;
        MarketEvent* pWrite = m_hot_buffer.get_write_ptr(cnt_write);
        if(cnt_write > 0)
        //if (cnt_write == size_batch) // if less - skip and wait
        {
            uint64_t tick_batch = rdtsc();


            for (int i = 0; i < cnt_write; ++i) {

                int ind = fast_rand_range(COIN_CNT);

                if (ind < 0 || ind >= COIN_CNT)
                {
                    _mm_pause();
                    continue;
                }

                auto& coin = coins[ind];


                // time
                if (cnt_tm_upd++ >= 50'000'000)
                {
                    cnt_tm_upd = 0;
                    batch_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()
                    ).count();
                }


                {
                    MarketEvent& ev = pWrite[i];


                    ev.index_symbol = ind;

                    ev.timestamp = batch_ts;

                    ev.price = coin.price + fast_rand_float_range(0, 0.7);

                    if (cnt_whale_gen++ >= 75'000'000)
                    {
                        cnt_whale_gen = 0;

                        ev.quantity = qty[ev.index_symbol] + fast_rand_range(qty_rnd[ev.index_symbol]) + fast_rand_float_range(0, 0.99);
                    }
                    else
                    {
                        ev.quantity = 1;
                    }

                    ev.is_sell = ((i & 1) == 0); //(i % 2 == 0)

                    ev.tick_rcvd = tick_batch;

                    cnt++;


                        // need for stable mode (low latency), else - Cache Coherency Traffic Storm

                    ////if(cnt & 7 == 0)  // 1 per 8 
                    ////if (cnt & 127 == 0) // 1 per 128
                    ////if (cnt % 2 == 0) // with it : Throughput: 94-97M     P50: 339-679 ns P99: 1018-3056 ns   (WSL at Win10 core-i7)
                    if (cnt % 10 == 0)    // with it : Throughput: 110-120M   P50: 341 ns     P99: 682 ns         (Win10 core-i7)
                    {
                        _mm_pause();   
                    }
                }

            }

            m_hot_buffer.commit_write(cnt_write);
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

#ifdef _WIN32
    set_affinity(m_monitor, 5);
#else
    set_affinity(pthread_self(), 7);
#endif

    uint64_t last_head = m_hot_buffer.get_head();
    auto last_time = std::chrono::steady_clock::now();

    int cnt = 0;

    while (m_running) 
    {

        std::this_thread::sleep_for(std::chrono::seconds(1));
        cnt++;

        uint64_t current_head = m_hot_buffer.get_head();
        auto current_time = std::chrono::steady_clock::now();

        if (!m_data_emulation)
        {
            uint64_t delta = current_head - last_head;

            std::chrono::duration<double> duration = current_time - last_time;
            double seconds = duration.count();

            double speed = delta / seconds;

            double eps = (speed >= 1e6) ? speed / 1e6 : (speed >= 1e3) ? speed / 1e3 : speed;
            double total = (current_head >= 1e6) ? current_head / 1e6 : (current_head >= 1e3) ? current_head / 1e3 : current_head;
            std::string mul = (speed >= 1e6) ? " M" : (speed >= 1e3) ? " K" : "";

            std::stringstream ss;
            ss << std::fixed << std::setprecision(2) << eps << mul << " event/sec | " << "Total: " << current_head << " events";

            std::cout << "\r" << "Throughput: " << std::left << std::setw(100) << ss.view() << std::flush;
        }
        else
        {
            // Throughput and statistics 

            static uint64_t last_total_count = 0;
            uint64_t current_total = stat_latency.count.load(std::memory_order_relaxed);
            uint64_t diff = current_total - last_total_count;
            last_total_count = current_total;

            uint64_t c = stat_latency.count.exchange(0);
            uint64_t t_ticks = stat_latency.total_ticks.exchange(0);
            //uint64_t mx_ticks = stat_latency.max_ticks.exchange(0);
            last_total_count = 0; // Resetting because count reached zero


            double eps = (diff >= 1e6) ? diff / 1e6 : (diff >= 1e3) ? diff / 1e3 : diff;
            std::string mul = (diff >= 1e6) ? " M" : (diff >= 1e3) ? " K" : "";

            std::stringstream ss;

            ss << std::fixed << std::setprecision(2) << eps << mul << " event/sec | " << "Total: " << current_head << " events";

            // skip first 3 sec to ignore initialization garbage
            if (cnt < 3) {
                last_head = current_head;
                last_time = current_time;
                continue;
            }

            if (c > 0) 
            {
                // k - for ticks to ns
                double tsc_to_ns = 1.0 / m_cpu_ghz;

                double avg_ns = (static_cast<double>(t_ticks) / c) * tsc_to_ns;
                //double max_ns = static_cast<double>(mx_ticks) * tsc_to_ns;

                ss << " | Avg: " << std::setprecision(1) << avg_ns << " ns";
                //ss << " Max: " << (uint64_t)max_ns << " ns |";

                // Calculating percentiles from buckets
                if (stat_latency.snapshot_ready.load(std::memory_order_acquire)) 
                {
                    uint64_t total_ev_in_snapshot = 0;
                    for (size_t i = 0; i < 4096; ++i) 
                    {
                        total_ev_in_snapshot += stat_latency.buckets_snapshot[i];
                    }

                    if (total_ev_in_snapshot > 0) 
                    {
                        uint64_t acc = 0;
                        uint64_t p50_ns = 0, p99_ns = 0, p999_ns = 0;

                        for (size_t i = 0; i < 4096; ++i) 
                        {
                            acc += stat_latency.buckets_snapshot[i];

                            // thresholds 50%, 99% è 99.9%
                            if (p50_ns == 0 && acc >= total_ev_in_snapshot * 0.50) {
                                // Convert index i to ticks (i << SHIFT) and then to nanoseconds
                                p50_ns = static_cast<uint64_t>((i << stat_latency.BUCKET_SHIFT) * tsc_to_ns);
                            }
                            if (p99_ns == 0 && acc >= total_ev_in_snapshot * 0.99) {
                                p99_ns = static_cast<uint64_t>((i << stat_latency.BUCKET_SHIFT) * tsc_to_ns);
                            }
                            if (p999_ns == 0 && acc >= total_ev_in_snapshot * 0.999) {
                                p999_ns = static_cast<uint64_t>((i << stat_latency.BUCKET_SHIFT) * tsc_to_ns);
                            }
                        }

                        ss << " P50: " << p50_ns << " ns";
                        ss << " P99: " << p99_ns << " ns";
                        ss << " P99.9: " << p999_ns << " ns";

                        // non-empty last bucket indicates outliers exceeding 1.2 ms
                        if (stat_latency.buckets_snapshot[4095] > 0) {
                            ss << " [!] Outliers: " << stat_latency.buckets_snapshot[4095];
                        }
                    }

                    stat_latency.snapshot_ready.store(false, std::memory_order_release);
                }
            }

            std::cout << "\r" << "Throughput: " << std::left << std::setw(140) << ss.view() << std::flush;
        }


        last_head = current_head;
        last_time = current_time;
    }
}


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

inline void Server::parse_single_event(simdjson::dom::element item) 
{
    MarketEvent event;

   
    std::string_view s;
    if (item["s"].get(s) == simdjson::error_code::SUCCESS) // 's' - it's deal
    {
        // Timestamp
        event.timestamp = item["E"].get_uint64();

        event.index_symbol = m_reg_coin.get_index_coin(s.data());

        if (event.index_symbol != -1)
        {
            // Price & Quantity
            std::string_view p_str = item["p"].get_string();
            std::from_chars(p_str.data(), p_str.data() + p_str.size(), event.price);

            std::string_view q_str = item["q"].get_string();
            std::from_chars(q_str.data(), q_str.data() + q_str.size(), event.quantity);

            // Side
            event.is_sell = item["m"].get_bool();

            uint64_t trade_id = item["t"].get_uint64();


            if (event.timestamp > 0)
            {
                m_hot_buffer.push_batch(&event, 1);
            }
        }
        else
        {
            ////  ooops.. unregistered coin..
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

    m_need_reset_vwap.store(true, std::memory_order_release);

    while (m_running)
    {
        ix::WebSocket ws;
        ws.setUrl(url);

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
                    //std::cerr << "\n[Binance] Closed by server\n";
                    return;
                }

                case ix::WebSocketMessageType::Error:
                {
                    std::cerr << "\n[Binance] Error: "
                        << msg->errorInfo.reason << "\n";
                    return;
                }

                default:
                    break;
                }
            });


        ws.setPingInterval(15);

        ws.start();


        uint64_t last_cnt = 0;
        while (m_running)
        {
            // pause without blocking shutdown 
            uint64_t sleep_dur = 5000; //5s
            uint64_t ts1 = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

            while (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() - ts1 < sleep_dur) {
                if (!m_running) {
                    break;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }



            uint64_t cur_cnt = ws_in_cnt.load();
            if (cur_cnt == last_cnt)
            {
                //std::cerr << "\n[Binance] No data,  reconnect\n";
                  break;
            }
            last_cnt = cur_cnt;
        }

        if (m_running)
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            std::cerr << "\n[Binance] Reconnecting...\n";

            m_need_reset_vwap.store(true, std::memory_order_release);
        }

    }
}

void Server::hot_dispatcher() 
{
    #ifdef _WIN32
        set_affinity(m_hot_dispatcher, 2);
        SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
    #else
        set_affinity(pthread_self(), 4);
        setpriority(PRIO_PROCESS, 0, -20);
    #endif

    bool ext_vwap = m_ext_vwap.load(std::memory_order_acquire);

    uint64_t reader_idx = m_hot_buffer.get_tail();
    uint64_t last_tail_update = reader_idx;
    uint64_t cached_h = reader_idx;

    uint64_t local_total_ticks = 0;
    uint64_t local_count = 0;
    uint64_t local_buckets[4096] = { 0 };

    while (m_running) 
    {
        // we only read Head when we have actually processed all the old stuff
        if (reader_idx >= cached_h) 
        {
            cached_h = m_hot_buffer.get_head();
            if (cached_h == reader_idx) {
                _mm_pause();
                continue;
            }
        }

        size_t avail_read = cached_h - reader_idx;
        //size_t to_process = (avail_read > 1024) ? 1024 : avail_read;
        size_t to_process = (avail_read > 64) ? 64 : avail_read;


        if (!m_event_buffer.can_write(to_process)) 
        {
            _mm_pause();
            continue;
        }

        uint64_t batch_now = __rdtsc();
        WhaleEvent* write_ptr = m_event_buffer.get_write_ptr();
        size_t whales_found = 0;

        for (size_t i = 0; i < to_process; ++i) {
            const auto& ev = m_hot_buffer.read(reader_idx++);

//            // PREFETCH coin_VWAP for 16 steps
//            if (i + 16 < to_process) 
//            {
//                uint32_t next_sym = m_hot_buffer.read(reader_idx + 16).index_symbol;
//
//#if defined(_MSC_VER)
//                _mm_prefetch((const char*)&coin_VWAP[next_sym], _MM_HINT_T0);
//#else
//                __builtin_prefetch(&coin_VWAP[next_sym], 1, 3);
//#endif
//            }

            auto& c = coin_VWAP[ev.index_symbol];
            c.session.add(ev.price, ev.quantity);
            if (ext_vwap) 
                c.roll50.add(ev.price, ev.quantity);

            uint64_t lat_ticks = batch_now - ev.tick_rcvd;
            local_total_ticks += lat_ticks;
            size_t b_idx = static_cast<size_t>(lat_ticks >> 10);
            local_buckets[(b_idx > 4095) ? 4095 : b_idx]++;
            local_count++;

            // Whale 
            if (ev.price * ev.quantity >= whale_global_treshold[ev.index_symbol]) [[unlikely]] 
            {
                WhaleEvent& we = write_ptr[whales_found++];
                we.index_symbol = ev.index_symbol;
                we.price = ev.price;
                we.quantity = ev.quantity;
                we.vwap_sess = c.session.value();
                if (ext_vwap)
                {
                    we.vwap_roll50 = c.roll50.value();
                    we.delta_roll = ev.price - we.vwap_roll50;
                }
            }
        }


        if (whales_found > 0) 
        {
            m_event_buffer.commit_write(whales_found);
        }

        if (reader_idx - last_tail_update >= 1024 /*65536*/) 
        {
            m_hot_buffer.update_tail(reader_idx);
            last_tail_update = reader_idx;

            // Reset and sync local stats every 10M events
            if (local_count >= 10'000'000)
            {
                stat_latency.total_ticks.fetch_add(local_total_ticks, std::memory_order_relaxed);
                stat_latency.count.fetch_add(local_count, std::memory_order_relaxed);

                //uint64_t current_max = stat_latency.max_ticks.load(std::memory_order_relaxed);
                //while (local_max_ticks > current_max &&
                //    !stat_latency.max_ticks.compare_exchange_weak(current_max, local_max_ticks));
                if (!stat_latency.snapshot_ready.load(std::memory_order_relaxed)) {
                    std::memcpy(stat_latency.buckets_snapshot, local_buckets, sizeof(local_buckets));
                    std::memset(local_buckets, 0, sizeof(local_buckets));
                    stat_latency.snapshot_ready.store(true, std::memory_order_release);
                }

                local_total_ticks = 0;
                local_count = 0;
            }
        }

        //if (to_process > 0) {
        //    empty_cycles = 0;
        //    //_mm_pause(); // No!
        //}
        //else {
        //    if (reader_idx != last_tail_update) {
        //        m_hot_buffer.update_tail(reader_idx);
        //        last_tail_update = reader_idx;
        //    }

        //    empty_cycles++;
        //    if (empty_cycles < 1000) {
        //        _mm_pause();
        //    }
        //    else if (empty_cycles < 50000) {
        //        for (int j = 0; j < 10; ++j)
        //            _mm_pause();
        //    }
        //    else if (empty_cycles < 100000) {
        //        std::this_thread::yield();
        //    }
        //    else {
        //        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        //    }
        //}

    }
}


void Server::event_dispatcher()
{

#ifdef _WIN32
    set_affinity(m_event_dispatcher, 4);
    SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
#else
    set_affinity(pthread_self(), 6);
    setpriority(PRIO_PROCESS, 0, -20);
#endif

    // for hold - prevent row_ptr invalidation 
    std::vector<std::shared_ptr<Session>> clients_shared; 

    // for fast send by session_index_symbol
    std::vector<std::vector<Session*>> clients_row;    // [ind_coin][Session*]
    clients_row.resize(COIN_CNT);

    uint64_t upd_tick = 0;
    int empty_cycles = 0;

    uint64_t reader_idx = m_event_buffer.get_head();
    uint64_t last_tail_update = reader_idx;
    while (m_running)
    {
        uint64_t h = m_event_buffer.get_head();

        uint64_t avail_read = h - reader_idx;


        if (avail_read > m_event_buffer.capacity() * 0.9) {
            reader_idx = h;
            m_event_buffer.update_tail(reader_idx);
            printf("\nevent_dispatcher OVERLOADED! DROPS!\n");
        }
        


        size_t to_process = (avail_read < 1024) ? avail_read : 1024;

        if (h > reader_idx)
        {

            //////////////////////////////////////////////////////////////
            // update local_clients 
            if (m_need_update_clients.load(std::memory_order_acquire) || upd_tick++ > 10'000'000'000)
            {
                upd_tick = 0;

                std::lock_guard<std::mutex> lock(m_mtx_subscribers);
                m_need_update_clients.store(false, std::memory_order_relaxed);

                const size_t rsrv_cnt = m_subscribers.size() + 10;

                clients_shared.clear();
                if (clients_shared.capacity() < rsrv_cnt)
                    clients_shared.reserve(rsrv_cnt);

                for (int i = 0; i < COIN_CNT; i++)
                {
                    clients_row[i].clear();
                    if (clients_row[i].capacity() < rsrv_cnt) {
                        clients_row[i].reserve(rsrv_cnt);
                    }
                }

                for (auto& sp : m_subscribers)
                {
                    clients_shared.push_back(sp);

                    Session* pSession = sp.get();
                    int ind = pSession->GetSymbolIndex();
                    if (ind >= 0 && ind < COIN_CNT)
                        clients_row[ind].push_back(pSession);
                }
            }
            //
            //////////////////////////////////////////////////////////////


            // send events
            for (size_t i = 0; i < to_process; ++i)
            {
                const auto& ev = m_event_buffer.read(reader_idx++);

                if (ev.index_symbol >= 0 && ev.index_symbol < COIN_CNT)
                {
                    auto& clients = clients_row[ev.index_symbol];
                    for (auto pSession : clients)
                    {
                        if (ev.total_usd() >= pSession->GetWhaleTreshold())
                        {
                            pSession->PushEvent(ev);
                        }
                    }
                }

            }

        }

        if (to_process > 0) {
            empty_cycles = 0;
            //_mm_pause();

            if (reader_idx - last_tail_update >= 512/*1024*/)
            {
                m_event_buffer.update_tail(reader_idx);
                last_tail_update = reader_idx;
            }
        }
        else {

            if (reader_idx != last_tail_update) {
                m_event_buffer.update_tail(reader_idx);
                last_tail_update = reader_idx;
            }

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
