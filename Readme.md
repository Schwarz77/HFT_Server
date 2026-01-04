# High-Frequency Market Data Analytics Engine

A low-latency C++ engine designed to process market data at 100M+ Events Per Second (EPS). The system performs real-time VWAP calculations, adaptive whale detection, and contextual data enrichment.
##🚀 Key Architectural Decisions
1. Zero-Copy Data Pipeline

To handle extreme throughput on consumer-grade hardware (like i7-9700), the system avoids the "Copy-Per-Subscriber" bottleneck.

    Shared State: All analytics (VWAP, Volume) are computed once in a global registry.

    Non-Blocking Dispatch: Events are pushed to sessions using a try_push strategy, ensuring that a slow network client never stalls the core processing engine.

2. Multi-Stage Adaptive Filtering

Instead of a static threshold, the engine uses a liquidity-aware filtering funnel:

    System Floor: Initial noise reduction to protect the CPU cache.

    Adaptive Threshold: The server dynamically calculates "Whale" levels for each asset based on a rolling 1-minute volume window (Threshold=AvgTrade×Multiplier×SafetyMargin).

    User Context: Subscribers receive a pre-filtered stream and apply their final granular filters.

3. Cache-Optimized Analytics (O(1))

The CoinAnalytics engine is designed around CPU cache line alignment (alignas(64)):

    Incremental Updates: VWAP is updated in-place without storing historical trade lists.

    Sliding Window: Uses a Bucketed Ring Buffer for 1-minute rolling statistics, ensuring constant time complexity regardless of event volume.

4. Asynchronous Network I/O (Boost.Asio)

To handle multiple concurrent subscribers without blocking the core engine, the system utilizes an asynchronous I/O model:

    Decoupled Architecture: The Core Dispatcher and the Network Layer communicate via lock-free buffers. Network latency or TCP backpressure never affects the analytics throughput.

    Efficient Broadcasting: Leveraging boost::asio to manage hundreds of simultaneous WebSocket/TCP connections using a thread-safe, non-blocking delivery mechanism.

    Pro-level Networking: Implementation of asynchronous write chains to ensure orderly data delivery without memory bloat.

##🏗 System Design

    The Dispatcher: The "Hot Loop" that owns the analytics state and performs event enrichment.

    Enriched Events: Whale alerts are not just price/size; they are enriched with 1m Momentum and Daily Baseline VWAP context at the moment of detection.

    Backpressure Management: Implementation of a "Drop-on-Overflow" policy for individual sessions to maintain overall system stability.

##🛠 Tech Stack

    Language: C++20

    Concurrency: Lock-free SPSC Queues, Atomic Operations.

    Optimization: Data-Oriented Design (DOD), Cache-line padding, SIMD-friendly loops.
	

## License

This project is licensed under the **MIT License**.	