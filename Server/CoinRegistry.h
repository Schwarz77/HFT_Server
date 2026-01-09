#pragma once

#include <string>

struct CoinNode {
    uint64_t key = 0;
    int index = -1;  
};

class CoinRegistry {
public:
    static constexpr size_t MASK = 8191;
    CoinNode table[8192];

    CoinRegistry() {
        std::memset(table, 0, sizeof(table));
    }

    inline uint64_t symbol2u64(const char* s) const
    {
        uint64_t s64 = 0;
        std::memcpy(&s64, s, std::min<size_t>(std::strlen(s), (size_t)8));

        return s64;

    }

    void register_coin(const char* symbol, int idx) {
        uint64_t s1 = symbol2u64(symbol);

        uint32_t slot = calculate_slot(s1);

        // find whole
        while (table[slot].key != 0) {
            if (table[slot].key == s1) {
                table[slot].index = idx; // update exists
                return;
            }
            slot = (slot + 1) & MASK;
        }

        table[slot].key = s1;
        table[slot].index = idx;
    }

    inline int get_index_coin(const char* symbol) const {
        return get_index_fast(symbol2u64(symbol));
    }

    inline int get_index_fast(uint64_t s1) const {
        uint32_t slot = calculate_slot(s1);

        while (table[slot].key != 0) {
            if (table[slot].key == s1) [[likely]] {
                return table[slot].index;
            }
            slot = (slot + 1) & MASK;
        }
        return -1;
    }

private:
    static inline uint32_t calculate_slot(uint64_t val) {
        uint64_t h = val;
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdLLU;
        h ^= h >> 33;
        return static_cast<uint32_t>(h & MASK);
    }
};