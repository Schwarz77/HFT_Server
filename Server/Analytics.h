#pragma once

#include <stdint.h>


struct alignas(64) CoinPair
{
    char symbol[16];
    double price;
};


struct alignas(16) PriceQty {
    double pv;
    double v;
};

template<int N>
struct RollingVWAP {
    alignas(64) PriceQty data[N] = { 0 };
    int pos = 0;
    double sum_pv = 0.0;
    double sum_v = 0.0;
    
    inline void add(double price, double qty) {
        const double x = price * qty;
        sum_pv += (x - data[pos].pv);
        sum_v += (qty - data[pos].v);
        data[pos] = { x, qty };
        if (++pos >= N) [[unlikely]] pos = 0;
    }

    inline double value() const {
        return (sum_v > 0.0000001) ? (sum_pv / sum_v) : 0.0;
    }

    inline void reset() {
        for (int i = 0; i < N; ++i) data[i].pv = data[i].v = 0.0;
        sum_pv = sum_v = 0.0;
        pos = 0;
    }
};

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

struct alignas(64) CoinAnalytics {
    SessionVWAP          session;
    RollingVWAP<50>      roll50;
    //double signed_flow = 0;

    CoinAnalytics()
    {
        session.reset();
        roll50.reset();
    }
};