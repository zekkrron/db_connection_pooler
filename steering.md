# Project Core: High-Concurrency Database Connection Pooler (Middleware)

**Target Paradigm:** Highly Concurrent, Low-Latency Asynchronous I/O
**Primary Language:** Java 
**Design Philosophy:** Mechanical Sympathy, Strict OOP, Zero-GC Hot Paths

## 1. Global Engineering Directives (Strict Constraints)

* **Zero Blocking I/O:** The system must use `java.nio` (specifically `Selector`, `SocketChannel`, and `ServerSocketChannel`). Standard `java.net.Socket` and `InputStream.read()` are strictly forbidden.
* **Lock-Free Hot Path:** The use of the `synchronized` keyword, `ReentrantLock`, or any OS-level Mutex is strictly forbidden in the request-routing hot path. Concurrency must be managed via hardware-level primitives (CAS, `AtomicReferenceArray`, `VarHandle`, or Memory Barriers).
* **Zero-Allocation Event Loop:** To prevent Garbage Collection (GC) Stop-The-World pauses, the system must not allocate new objects during the hot path. 
* **Off-Heap Memory:** All network byte reading and writing must use `ByteBuffer.allocateDirect()` to utilize OS RAM directly and bypass the JVM heap.
* **Immutable State:** Use the `final` keyword aggressively for all shared configuration and connection state variables to prevent thread corruption.

## 2. System Architecture & Threading Model

The system operates as a standalone proxy between client applications and the backend database cluster.

* **Worker Threads (The Event Loops):** The system will spawn a fixed number of Worker Threads (defaulting to `Runtime.getRuntime().availableProcessors()`). Each Worker Thread runs its own infinite `Selector` loop.
* **Direct I/O Routing:** The Worker Thread handles both reading the incoming client SQL payload and directly writing it to the acquired database socket. There is no separate "sweeper" or "sender" thread. The data flows seamlessly from Client Socket -> Worker Thread Memory -> Database Socket.
* **The Janitor Thread:** A single, isolated background daemon thread. It wakes up periodically to scan the internal connection pools, send lightweight pings to database sockets, and seamlessly destroy/replace `STALE` connections without interrupting the Worker Threads.

## 3. Object-Oriented Design (LLD)

### 3.1. The Strategy Pattern for Connection Pools
The core concurrency mechanism must be abstracted. Create a `ConnectionPool` interface. The Worker Threads must program against this interface. Implement two distinct versions:

1.  `CasWaitQueue`: A lock-free pool using hardware Compare-And-Swap. Threads will compete for a shared "next available" pointer.
2.  `RingBufferPool`: An LMAX Disruptor-style circular array. Use independent sequence numbers and aggressive Cache Line Padding (inserting 7 dummy `long` variables between sequence counters) to completely eliminate False Sharing and hardware-level cache bouncing.

### 3.2. Query-Aware Routing
Implement a `ProtocolParser` interface. The Worker Thread will pass the raw `ByteBuffer` to this parser.
* If the SQL payload begins with `SELECT`, the router must acquire a connection from a "Read Replica" `ConnectionPool`.
* If the payload begins with `INSERT`, `UPDATE`, or `DELETE`, the router must acquire a connection from the "Master" `ConnectionPool`.

### 3.3. State Machine Strictness
Database connection wrappers must maintain a strict, atomic state machine: `IDLE` -> `BUSY` -> `STALE` -> `DESTROYED`. State transitions must be thread-safe.

---

## 4. Execution Roadmap (Phase-by-Phase)

* **Phase 1 (Foundation):** Implement the `java.nio.Selector` Event Loop. Prove a single Worker Thread can accept 10,000 dummy client connections and read their byte buffers without crashing or blocking.
* **Phase 2 (The Interfaces):** Define the `ConnectionPool`, `ProtocolParser`, and `DatabaseConnection` interfaces. 
* **Phase 3 (CAS Implementation):** Build the `CasWaitQueue` implementing the `ConnectionPool` interface. 
* **Phase 4 (The Janitor):** Implement the background health-check daemon and the connection State Machine.
* **Phase 5 (Ring Buffer Upgrade):** Build the `RingBufferPool` implementation with Cache Line Padding. Map the configuration to easily swap between CAS and Ring Buffer on startup.

---

## 5. Design Discussion: Shared vs. Thread-Local Buffer Pools (TODO)

**Current State:** All Worker Threads pull from a single shared `DirectBufferPool`. It is lock-free (CAS-based) and fast, but threads still compete for buffer slots at the hardware level — every `getAndSet` / `compareAndSet` on the shared `AtomicReferenceArray` causes cache line traffic between cores.

**The Thread-Local Optimization:** In hyper-optimized systems like Netty (which powers Cassandra, Elasticsearch, and most high-throughput Java infrastructure), each Worker Thread owns a private buffer pool. If Thread 1 only ever touches Buffer Pool 1, there is literally zero cross-core contention — no CAS, no cache bouncing, no false sharing. The buffer acquire/release becomes a plain array index read/write on a single core.

**How it would work in our codebase:**
- Instead of passing a shared `DirectBufferPool` into `EventLoopGroup`, each `EventLoop` would own its own `DirectBufferPool` (or a simpler non-atomic array, since no other thread touches it).
- The `EventLoop` constructor would allocate its private pool: `new DirectBufferPool(perThreadPoolSize, bufferCapacity)`.
- Since each event loop is pinned to one thread, no synchronization is needed at all — acquire/release becomes a plain array scan with no atomics.
- Total memory stays the same (just partitioned), but throughput under contention improves because cores never invalidate each other's cache lines for buffer operations.

**Trade-offs to discuss:**
- Simpler per-thread code (no CAS needed) vs. slightly uneven memory utilization if some threads are busier than others.
- A shared pool can absorb burst traffic on any thread; per-thread pools can starve if one thread gets a spike. Netty solves this with a fallback to a shared "arena" when the thread-local pool is exhausted.
- Implementation complexity is low — the main change is in `EventLoopGroup` and `EventLoop` construction.

**Decision:** To be discussed. May or may not implement, but understanding this trade-off is important for interviews and for knowing where the next level of optimization lives.

---

## 6. Testing & Benchmarking Strategy (NOTE: For execution after Phase 5)

The system must be proven under enterprise-level load. The testing architecture is as follows:

* **Concurrency Correctness:** Use `jcstress` (Java Concurrency Stress) to aggressively hammer the `CasWaitQueue` and `RingBufferPool` classes with dozens of threads to prove the lock-free math is free of race conditions and deadlocks.
* **Load Generation:** Use `pgbench` (or `sysbench`) to simulate 10,000 concurrent client connections. 
* **Database Bottleneck Control:** The backend database being tested against must be incredibly small (e.g., a simple primary key lookup on a 10,000-row table, or a `SELECT 1;` ping). The database must return results in <0.1ms to ensure the physical bottleneck is forcefully shifted to the Java middleware's CPU and memory routing capabilities.
* **Telemetry:** Integrate Micrometer to track P99 checkout latency, active connections, and queue depth. Expose a `/metrics` endpoint formatted for Prometheus scraping, intended for Grafana visualization.