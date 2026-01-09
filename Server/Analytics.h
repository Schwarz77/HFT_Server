#pragma once

#include <stdint.h>


struct CoinPair
{
    char symbol[16];
    double price;
    //double global_whale_treshold;
};


template<int N>
struct RollingVWAP {
    double pv[N];
    double v[N];
    int    pos = 0;
    int    cnt = 0;

    double sum_pv = 0.0;
    double sum_v = 0.0;

    inline void add(double price, double qty) {
        double x = price * qty;

        if (cnt < N) {
            pv[pos] = x;
            v[pos] = qty;
            sum_pv += x;
            sum_v += qty;
            cnt++;
        }
        else {
            sum_pv += x - pv[pos];
            sum_v += qty - v[pos];
            pv[pos] = x;
            v[pos] = qty;
        }

        pos++;
        if (pos == N) pos = 0;
    }

    inline double value() const {
        return (sum_v > 0.0) ? (sum_pv / sum_v) : 0.0;
    }

    inline void reset() {
        pos = cnt = 0;
        sum_pv = sum_v = 0.0;
    }
};

////const uint64_t EWMA_RESET_GAP = 2500;   // crypto trades - 2–5sec
//const uint64_t EWMA_RESET_GAP = 500;  // HFT: 100–500 ms
//
//struct EWMAVWAP {
//    double vwap = 0.0;
//    bool   init = false;
//    uint64_t last_trade_ts = 0;
//
//    // alpha ~ 0.05–0.15 for whale
//    inline void add(double price, double alpha, uint64_t ts) {
//        if (!init) {
//            vwap = price;
//            init = true;
//            last_trade_ts = ts;
//        }
//        else {
//            if (ts - last_trade_ts > EWMA_RESET_GAP) {
//                reset();
//            }
//            else {
//                vwap = alpha * price + (1.0 - alpha) * vwap;
//            }
//            last_trade_ts = ts;
//        }
//    }
//
//    inline double value() const {
//        return vwap;
//    }
//
//    inline void reset() {
//        init = false;
//        vwap = 0.0;
//    }
//};

//constexpr double tau = 5.0; // seconds
//
//struct EWMAVWAP {
//    double value = 0.0;
//    uint64_t last_ts = 0;
//
//    inline void update(double price, uint64_t ts_ms)
//    {
//        if (last_ts == 0) {
//            value = price;
//            last_ts = ts_ms;
//            return;
//        }
//
//        double dt = (ts_ms - last_ts) * 0.001;
//        last_ts = ts_ms;
//        double alpha = 1.0 - std::exp(-dt / tau);
//
//        value += alpha * (price - value);
//    }
//};


struct SessionVWAP {
    double pv = 0.0;
    double v = 0.0;

    inline void add(double price, double qty) {
        pv += price * qty;
        v += qty;
    }

    inline double value() const {
        return (v > 0.0) ? (pv / v) : 0.0;
    }

    inline void reset() {
        pv = v = 0.0;
    }
};

struct CoinAnalytics {
    SessionVWAP          session;
    RollingVWAP<50>      roll50;
    //EWMAVWAP             ewma;
    //double signed_flow = 0;

    CoinAnalytics()
    {
        session.reset();
        roll50.reset();
    }
};