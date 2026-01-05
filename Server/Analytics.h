#pragma once

#include <stdint.h>


struct CoinPair
{
    char symbol[16];
    double price;
};


struct CoinVWAP
{
    // 1 day
    double day_pv = 0.0;
    double day_v = 0.0;

    // 1 min sliding
    struct Bucket { double pv, v; };
    Bucket buckets[60] = {};
    double min_pv = 0.0;
    double min_v = 0.0;

    uint64_t last_sec = 0;
    uint8_t  cursor = 0;
};

struct SecAgg {
    uint64_t sec;
    double pv;
    double v;
};

constexpr int MAX_SEC_IN_BATCH = 8;

inline void apply_batch(
    CoinVWAP& c,
    double batch_pv,
    double batch_v,
    uint64_t ts_sec)
{
    // day
    c.day_pv += batch_pv;
    c.day_v += batch_v;

    // minute sliding
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

    c.buckets[c.cursor].pv += batch_pv;
    c.buckets[c.cursor].v += batch_v;
    c.min_pv += batch_pv;
    c.min_v += batch_v;
}

inline double vwap_1m(const CoinVWAP& c) {
    return c.min_v > 0 ? c.min_pv / c.min_v : 0.0;
}

inline double vwap_1d(const CoinVWAP& c) {
    return c.day_v > 0 ? c.day_pv / c.day_v : 0.0;
}