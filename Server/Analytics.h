#pragma once

#include <stdint.h>


struct CoinPair
{
    char symbol[16];
    double price;
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
    //double signed_flow = 0;

    CoinAnalytics()
    {
        session.reset();
        roll50.reset();
    }
};