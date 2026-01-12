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


///////////////////////////////////////////////////////////////////////
//
// 
        // it's global data. if move it to class member - speed will decrease slightly 

// const array - max speed
const CoinPair coins[] =
{
    {"BTCUSDT", 96000.0}, {"ETHUSDT", 2700.0}, {"SOLUSDT", 180.0}, {"BNBUSDT", 600.0}
};

const size_t COIN_CNT = _countof(coins);

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
    init_coin_data();
    register_coins();
}

Server::~Server() 
{
    Stop();
}

void Server::Start() 
{
    m_session_dispatcher = std::thread(&Server::session_dispatcher, this);
    m_hot_dispatcher = std::thread(&Server::hot_dispatcher, this);
    m_event_dispatcher = std::thread(&Server::event_dispatcher, this);
    m_monitor = std::thread(&Server::speed_monitor, this);
    m_producer = std::thread(&Server::producer, this);

    set_affinity(m_producer, 0);
    set_affinity(m_hot_dispatcher, 2);
    set_affinity(m_event_dispatcher, 4);
    set_affinity(m_monitor, 5);
    
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
    SetThreadAffinityMask(GetCurrentThread(), 1 << 0);

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
    std::vector<MarketEvent> batch(size_batch);

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

    m_need_reset_vwap.store(true, std::memory_order::release);

    while (m_running) 
    {
        if (m_hot_buffer.can_write(size_batch))
        {
            for (int i = 0; i < size_batch; ++i) {

                int ind = fast_rand_range(COIN_CNT);

                if (ind < 0 || ind >= COIN_CNT)
                {
                    _mm_pause();
                    continue;
                }

                auto& coin = coins[ind];


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

                    cnt++;
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

    m_need_reset_vwap.store(true, std::memory_order::release);

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

            m_need_reset_vwap.store(true, std::memory_order::release);
        }

    }
}


void Server::hot_dispatcher() 
{
    SetThreadAffinityMask(GetCurrentThread(), 1 << 2);

    uint64_t reader_idx = m_hot_buffer.get_tail();

    int empty_cycles = 0;

    std::vector<WhaleEvent> bach_to_client(1024);
    uint64_t cnt_event_to_client = 0;

    bool ext_vwap = m_ext_vwap.load(std::memory_order::acquire);

    const uint64_t overload_val = m_hot_buffer.capacity() * 0.9;

    uint64_t last_tail_update = reader_idx;

    while (m_running) {
        uint64_t h = m_hot_buffer.get_head();

        size_t avail_read = h - reader_idx;

        if (avail_read > overload_val) {
            reader_idx = h;
            m_hot_buffer.update_tail(reader_idx);
            printf("\nhot_dispatcher OVERLOADED! DROPS!\n");
        }


        size_t to_process = (avail_read < 1024) ? avail_read : 1024;

        if (h > reader_idx) 
        {
            for (size_t i = 0; i < to_process; ++i)
            {
                const auto& ev = m_hot_buffer.read(reader_idx++);

                if (ev.index_symbol < 0 || ev.index_symbol >= COIN_CNT)
                    continue;

                double pv = ev.total_usd();

                auto& c = coin_VWAP[ev.index_symbol];

                //if (m_need_reset_vwap.load(std::memory_order::acquire))
                //{
                //    c.session.reset();
                //    //c.signed_flow = 0;
                //}

                c.session.add(ev.price, ev.quantity);
                if (ext_vwap)
                {
                    c.roll50.add(ev.price, ev.quantity);
                    //c.signed_flow += ev.is_sell ? -ev.quantity : ev.quantity;
                }

                if (pv >= whale_global_treshold[ev.index_symbol]) [[unlikely]]
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
                        we.delta_roll = ev.price - we.vwap_roll50;
                    }

                }

            }

            if (reader_idx - last_tail_update >= 512/*1024*/)
            {
                m_hot_buffer.update_tail(reader_idx);
                last_tail_update = reader_idx;
            }

            if (cnt_event_to_client > 0) {

                //write events
                while (!m_event_buffer.can_write(cnt_event_to_client)) {
                    if (!m_running)
                        return;
                    _mm_pause();
                }

                m_event_buffer.push_batch(&bach_to_client[0], cnt_event_to_client);
                cnt_event_to_client = 0;
            }

        }
  

        if (to_process > 0) {
            empty_cycles = 0;
            //_mm_pause(); // No!
        }
        else {
            if (reader_idx != last_tail_update) {
                m_hot_buffer.update_tail(reader_idx);
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


void Server::event_dispatcher()
{
    SetThreadAffinityMask(GetCurrentThread(), 1 << 4);

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
