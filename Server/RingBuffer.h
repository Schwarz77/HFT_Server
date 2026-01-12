#pragma once

#include <atomic>
#include <vector>
#include <cstdint>


template<typename T, uint64_t Capacity>
class RingBuffer {
    static_assert((Capacity& (Capacity - 1)) == 0, "Capacity must be a power of 2");

public:
    RingBuffer() : buffer(Capacity) {
        head.store(0, std::memory_order_relaxed);
        tail.store(0, std::memory_order_relaxed);
    }

    T read(uint64_t idx) const {
        return buffer[idx & mask];
    }

    bool can_write(uint64_t count) const {
        static constexpr uint64_t HIGH_WATER = Capacity * 9 / 10;

        uint64_t h = head.load(std::memory_order_relaxed);
        uint64_t t = tail.load(std::memory_order_acquire);
        uint64_t used = h - t;

        return (used + count) <= HIGH_WATER;
    }

    void push_batch(const T* items, size_t count) {
        uint64_t h = head.load(std::memory_order_relaxed);
        uint32_t write_pos = h & mask;

        if (write_pos + count <= Capacity) {
            std::memcpy(&buffer[write_pos], items, count * sizeof(T));
        }
        else {
            size_t first_part = Capacity - write_pos;
            std::memcpy(&buffer[write_pos], items, first_part * sizeof(T));
            std::memcpy(&buffer[0], &items[first_part], (count - first_part) * sizeof(T));
        }

        head.store(h + count, std::memory_order_release);
    }

    void update_tail(uint64_t reader_idx) {
        tail.store(reader_idx, std::memory_order_release);
    }

    uint64_t get_head() const { return head.load(std::memory_order_acquire); }
    uint64_t get_tail() const { return tail.load(std::memory_order_acquire); }

    static constexpr uint64_t capacity() noexcept {
        return Capacity;
    }

private:

    alignas(64) std::atomic<uint64_t> head;
    std::vector<T> buffer;
    const uint64_t mask = Capacity - 1;
    alignas(64) std::atomic<uint64_t> tail;
};



//////////////////////////////////////////////////////////////////////////



template<typename T, uint64_t Size>
class SessionRingBuffer {
    static_assert((Size& (Size - 1)) == 0, "Power of 2");
    const uint64_t mask = Size - 1;
public:
    SessionRingBuffer() : buffer(Size) {
        head.store(0, std::memory_order_relaxed);
        tail.store(0, std::memory_order_relaxed);
    }

    bool try_push(const T& item) {
        const uint64_t curr_head = head.load(std::memory_order_relaxed);
        const uint64_t curr_tail = tail.load(std::memory_order_acquire);

        if (curr_head - curr_tail >= Size)
            return false;

        buffer[curr_head & (Size - 1)] = item;
        head.store(curr_head + 1, std::memory_order_release);
        return true;
    }

    bool force_push(const T& item) {
        const uint64_t h = head.load(std::memory_order_relaxed);
        const uint64_t t = tail.load(std::memory_order_acquire);

        if (h - t >= Size)
            return false;

        buffer[h & mask] = item;
        head.store(h + 1, std::memory_order_release);
        return true;
    }

    size_t pop_batch(T* out_array, size_t max_count) {
        const uint64_t h = head.load(std::memory_order_acquire);
        const uint64_t t = tail.load(std::memory_order_relaxed);

        if (t == h)
            return 0;

        size_t available = h - t;
        size_t to_read = (available < max_count) ? available : max_count;

        size_t start_idx = t & mask;

        size_t first_part = Size - start_idx;

        if (to_read <= first_part) {

            std::memcpy(out_array, &buffer[start_idx], to_read * sizeof(T));
        }
        else {
            std::memcpy(out_array, &buffer[start_idx], first_part * sizeof(T));

            size_t second_part = to_read - first_part;
            std::memcpy(out_array + first_part, &buffer[0], second_part * sizeof(T));
        }

        tail.store(t + to_read, std::memory_order_release);
        return to_read;
    }

    void update_tail(uint64_t reader_idx) {
        tail.store(reader_idx, std::memory_order_release);
    }

    uint64_t get_head() const { return head.load(std::memory_order_acquire); }

    static constexpr uint64_t capacity() noexcept {
        return Size;
    }

private:

    alignas(64) std::atomic<uint64_t> head;
    std::vector<T> buffer;
    alignas(64) std::atomic<uint64_t> tail;
};


