// RingBufferTest.cpp

#include <gtest/gtest.h>
#include "RingBuffer.h"
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>


TEST(RingBufferTest, HighSpeedConcurrency) {
    constexpr uint64_t CAPACITY = 1024 * 1024;
    constexpr size_t TOTAL_EVENTS = 50'000'000;

    RingBuffer<uint64_t, CAPACITY> buffer;

    std::atomic<bool> start_flag{false};
    std::atomic<uint64_t> received_sum{0};
    std::atomic<size_t> received_count{0};

    std::atomic<bool> write_finish{ false };

    // Producer thread
    std::thread producer([&]() {
        while (!start_flag) std::this_thread::yield();
        for (uint64_t i = 1; i <= TOTAL_EVENTS; ++i) {
            while (!buffer.can_write(1)) {
                _mm_pause(); // wait
            }
            buffer.push_batch(&i, 1);
        }

        write_finish.store(true, std::memory_order_release);
    });

    // Consumer thread
    std::thread consumer([&]() {
        while (!start_flag) std::this_thread::yield();
        uint64_t batch[1024];

        uint64_t reader_idx = buffer.get_head();

        while (received_count < TOTAL_EVENTS) {
            uint64_t h = buffer.get_head();

            size_t avail_read = h - reader_idx;

            if (avail_read > 0) {
                size_t to_process = (avail_read < 1024) ? avail_read : 1024;

                if (received_count + to_process > TOTAL_EVENTS) {
                    to_process = TOTAL_EVENTS - received_count;
                }

                for (size_t i = 0; i < to_process; ++i) {
                    received_sum += buffer.read(reader_idx++);
                }
                received_count += to_process;
                buffer.update_tail(reader_idx);
            }
            else {
 
                if (write_finish.load(std::memory_order_acquire)) {
                    break;
                }
                _mm_pause();
            }
        }

    });

    auto start_time = std::chrono::high_resolution_clock::now();
    start_flag = true;

    producer.join();
    consumer.join();
    auto end_time = std::chrono::high_resolution_clock::now();

    // calc speed
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    double eps = (TOTAL_EVENTS / (duration / 1000.0)) / 1'000'000.0;

    std::cout << "[          ] Speed: " << eps << " Million Events/sec" << std::endl;

    // Data integrity check
    // Sum of numbers from 1 to N = N*(N+1)/2
    uint64_t expected_sum = (uint64_t)TOTAL_EVENTS * (TOTAL_EVENTS + 1) / 2;
    EXPECT_EQ(received_count, TOTAL_EVENTS);
    EXPECT_EQ(received_sum, expected_sum);
}


TEST(SessionRingBufferTest, HighSpeedConcurrency2) {
    constexpr uint64_t CAPACITY = 1024 * 1024;
    constexpr size_t TOTAL_EVENTS = 50'000'000;

    auto buffer_ptr = std::make_unique<SessionRingBuffer<uint64_t, CAPACITY>>();
    auto& buffer = *buffer_ptr;

    std::atomic<bool> start_flag{ false };
    std::atomic<uint64_t> received_sum{ 0 };
    std::atomic<size_t> received_count{ 0 };

    // Producer thread
    std::thread producer([&]() {
        while (!start_flag) std::this_thread::yield();
        for (uint64_t i = 1; i <= TOTAL_EVENTS; ++i) {
            while (!buffer.force_push(i)) {
                _mm_pause(); // wait
            }
        }
        });

    // Consumer thread
    std::thread consumer([&]() {
        while (!start_flag) std::this_thread::yield();
        uint64_t batch[1024];
        while (received_count < TOTAL_EVENTS) {
            size_t n = buffer.pop_batch(batch, 1024);
            if (n > 0) {
                for (size_t i = 0; i < n; ++i) {
                    received_sum += batch[i];
                }
                received_count += n;
            }
            else {
                _mm_pause();
            }
        }
        });

    auto start_time = std::chrono::high_resolution_clock::now();
    start_flag = true;

    producer.join();
    consumer.join();
    auto end_time = std::chrono::high_resolution_clock::now();

    // calc speed
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    double eps = (TOTAL_EVENTS / (duration / 1000.0)) / 1'000'000.0;

    std::cout << "[          ] Speed: " << eps << " Million Events/sec" << std::endl;

    // Data integrity check
    // Sum of numbers from 1 to N = N*(N+1)/2
    uint64_t expected_sum = (uint64_t)TOTAL_EVENTS * (TOTAL_EVENTS + 1) / 2;
    EXPECT_EQ(received_count, TOTAL_EVENTS);
    EXPECT_EQ(received_sum, expected_sum);
}
