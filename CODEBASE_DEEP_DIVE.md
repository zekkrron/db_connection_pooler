# DBPooler — Complete Codebase Deep Dive

## The Big Picture

This project is a **database connection pooler** — a middleware proxy that sits between your application and a database (like PostgreSQL). Instead of every app request opening a fresh TCP connection to the database (which is slow), this pooler keeps a bunch of pre-opened connections ready to go and hands them out on demand.

The design goals from `steering.md` are aggressive:
- Zero locks on the hot path (use hardware-level atomic operations instead)
- Zero object allocation during request handling (to avoid garbage collection pauses)
- Off-heap memory (bypass the JVM's managed memory entirely)
- Non-blocking I/O everywhere

---

## Layer 1: The Connection Wrapper

### `DatabaseConnection.java` — The thing being pooled

```java
public enum State {
    IDLE, BUSY, STALE, DESTROYED
}
```

Every database connection has a strict state machine. Think of it like a traffic light — it can only be in one state at a time, and transitions are controlled.

- `IDLE` — sitting in the pool, available for use
- `BUSY` — a worker thread is actively using it
- `STALE` — the janitor found it unhealthy
- `DESTROYED` — socket is closed, game over

```java
private final AtomicReference<State> state;
```

**Java concept: `AtomicReference`** — This is a thread-safe container. Multiple threads can read/write it simultaneously without locks. It uses a CPU instruction called CAS (Compare-And-Swap): "if the current value is X, change it to Y, otherwise do nothing and tell me it failed."

```java
public boolean acquire() {
    return state.compareAndSet(State.IDLE, State.BUSY);
}
```

This says: "If the state is currently IDLE, atomically flip it to BUSY and return true. If someone else already grabbed it, return false." No locks needed — the CPU hardware handles the race condition.

```java
private final SocketChannel channel;
```

**Java concept: `SocketChannel`** — This is Java NIO's non-blocking TCP socket. Unlike the old `java.net.Socket` which blocks your thread while waiting for data, a `SocketChannel` can be set to non-blocking mode so your thread can check "is there data?" and move on if not.

The `write()` and `read()` methods are thin wrappers around the channel. The `destroy()` method is the terminal state — it closes the socket and can never be undone.

```java
public boolean markStale() {
    State current;
    do {
        current = state.get();
        if (current == State.DESTROYED) return false;
    } while (!state.compareAndSet(current, State.STALE));
    return true;
}
```

This is a CAS loop. It keeps trying to set the state to STALE regardless of what the current state is (IDLE or BUSY), but bails out if it's already DESTROYED. The `do/while` retries if another thread changed the state between our `get()` and our `compareAndSet()`.

---

## Layer 2: The Connection Pools

### `ConnectionPool.java` — The Strategy Interface

```java
public interface ConnectionPool {
    DatabaseConnection acquire();
    void release(DatabaseConnection connection);
    boolean offer(DatabaseConnection connection);
    boolean remove(DatabaseConnection connection);
    int size();
    int capacity();
}
```

**Java concept: `interface`** — This is a contract. It says "anything that claims to be a ConnectionPool must provide these 6 methods." The actual implementation can vary. This is the Strategy Pattern — the rest of the code programs against this interface and doesn't care which concrete pool is behind it.

Two implementations exist: `CasWaitQueue` and `RingBufferPool`.

---

### `CasWaitQueue.java` — Lock-free pool via CAS scanning

```java
private final AtomicReferenceArray<DatabaseConnection> slots;
private final AtomicInteger scanIndex;
```

**Java concept: `AtomicReferenceArray`** — Like a regular array, but every read/write/CAS on individual elements is atomic. Multiple threads can safely operate on different (or even the same) slots simultaneously.

The pool is a fixed-size array of connection slots. `scanIndex` is a shared pointer that tells threads where to start looking.

```java
public DatabaseConnection acquire() {
    final int start = Math.abs(scanIndex.getAndIncrement() % capacity);
    for (int i = 0; i < capacity; i++) {
        final int idx = (start + i) % capacity;
        final DatabaseConnection conn = slots.get(idx);
        if (conn != null && conn.acquire()) {
            return conn;
        }
    }
    return null;
}
```

Here's the clever part: each thread that calls `acquire()` gets a different starting index (because `scanIndex` increments atomically). This spreads threads across the array so they're not all fighting over slot 0. Then it wraps around the entire array looking for an IDLE connection. `conn.acquire()` is the CAS on the connection's state — if it returns true, this thread won it.

```java
public void release(final DatabaseConnection connection) {
    if (!connection.release()) return;
    // Connection is now IDLE and still sitting in its slot.
}
```

Release is beautifully simple here. The connection never leaves its slot — `release()` just flips its state from BUSY back to IDLE. The next `acquire()` scan will find it.

`offer()` is for adding brand new connections (during init or janitor replacement) — it CAS-scans for a `null` slot and drops the connection in. `remove()` does the reverse.

---

### `PaddedSequence.java` — Defeating False Sharing

This one is pure mechanical sympathy.

**The problem:** Modern CPUs don't read individual bytes from RAM — they load entire "cache lines" (64 bytes). If two threads are writing to two different variables that happen to sit on the same 64-byte cache line, the CPU constantly invalidates and reloads that line between cores. This is called "false sharing" and it murders performance.

```java
@SuppressWarnings("unused") private long p1, p2, p3, p4, p5, p6, p7;
private volatile long value;
@SuppressWarnings("unused") private long p9, p10, p11, p12, p13, p14, p15;
```

Each `long` is 8 bytes. 7 longs = 56 bytes of padding on each side. The `value` field (8 bytes) + padding = guaranteed to sit alone on its own cache line. The `@SuppressWarnings("unused")` tells the Java compiler "yes I know these fields are never read, that's the point."

```java
private static final VarHandle VALUE_HANDLE;
static {
    VALUE_HANDLE = MethodHandles.lookup()
            .findVarHandle(PaddedSequence.class, "value", long.class);
}
```

**Java concept: `VarHandle`** — This is Java 9+'s low-level way to do atomic operations on fields. It's faster than `AtomicLong` because it avoids an extra layer of indirection. The `static` block runs once when the class is loaded and creates a handle to the `value` field.

**Java concept: `static` block** — Code that runs exactly once when the JVM first loads this class. Used here to set up the VarHandle.

```java
public long getAndIncrement() {
    long current;
    do {
        current = get();
    } while (!compareAndSet(current, current + 1));
    return current;
}
```

Classic CAS loop: read the value, try to set it to value+1, retry if someone else changed it between the read and the write.

---

### `RingBufferPool.java` — LMAX Disruptor-style pool

This is the more sophisticated pool implementation, inspired by the LMAX Disruptor pattern used in high-frequency trading.

```java
private final int mask;
// ...
this.capacity = nextPowerOfTwo(requestedCapacity);
this.mask = capacity - 1;
```

The capacity is forced to a power of 2. Why? Because `index % capacity` (modulo) is slow — it requires integer division. But if capacity is a power of 2, `index & mask` (bitwise AND) gives the same result and is a single CPU instruction. For example, if capacity = 8 (binary `1000`), mask = 7 (binary `0111`), and `13 & 7 = 5`, which is the same as `13 % 8`.

```java
private final PaddedSequence consumerSeq;
private final PaddedSequence producerSeq;
```

Two independent sequence counters, each padded to its own cache line. `consumerSeq` tracks where `acquire()` reads from. `producerSeq` tracks where `release()`/`offer()` writes to. Because they're on separate cache lines, a thread doing `acquire()` doesn't cause cache invalidation for a thread doing `release()`.

```java
public DatabaseConnection acquire() {
    for (int spin = 0; spin < capacity; spin++) {
        final long seq = consumerSeq.getAndIncrement();
        final int idx = (int) (seq & mask);
        final DatabaseConnection conn = ring.getAndSet(idx, null);
        if (conn != null && conn.acquire()) {
            return conn;
        }
        if (conn != null) {
            ring.compareAndSet(idx, null, conn);
        }
    }
    return null;
}
```

Each consumer atomically claims a sequence number, maps it to a ring index, and atomically swaps the slot to `null` (taking the connection out). If the connection was acquirable, great. If not (it was BUSY/STALE), it tries to put it back with a CAS. The `getAndSet` + `compareAndSet` combo ensures no connection is lost even under heavy contention.

```java
public void release(final DatabaseConnection connection) {
    if (!connection.release()) return;
    final long seq = producerSeq.getAndIncrement();
    final int idx = (int) (seq & mask);
    if (!ring.compareAndSet(idx, null, connection)) {
        for (int i = 1; i < capacity; i++) {
            final int probe = (idx + i) & mask;
            if (ring.compareAndSet(probe, null, connection)) return;
        }
        connection.acquire(); // re-acquire so it's not orphaned as IDLE
    }
}
```

Release claims a producer sequence number, tries to CAS the connection into that slot. If the slot is occupied, it probes forward looking for an empty one. The fallback `connection.acquire()` at the end is a safety net — if the ring is completely full and we can't place the connection anywhere, we flip it back to BUSY so it's not left in a dangling IDLE state with no slot.

---

## Layer 3: Creating Connections

### `ConnectionFactory.java`

```java
final SocketChannel channel = SocketChannel.open();
channel.configureBlocking(false);
channel.connect(address);
while (!channel.finishConnect()) {
    Thread.onSpinWait();
}
```

Opens a raw TCP socket to the database server in non-blocking mode. `configureBlocking(false)` is the key NIO call — it makes all I/O operations return immediately instead of waiting. `connect()` starts the TCP handshake, and `finishConnect()` checks if it's done. `Thread.onSpinWait()` is a JVM hint to the CPU that we're in a spin loop — on modern x86 CPUs this emits a `PAUSE` instruction that reduces power consumption and improves performance of the spinning core.

---

## Layer 4: The Protocol Layer

### `QueryType.java`

Simple enum: `READ`, `WRITE`, `UNKNOWN`. Used to decide which pool a query goes to.

### `ProtocolParser.java`

The interface contract: given a `ByteBuffer` of raw bytes, tell me what kind of query this is.

### `SqlProtocolParser.java`

```java
private static final byte[] SELECT = {'S', 'E', 'L', 'E', 'C', 'T'};
```

Pre-computed byte arrays for keyword matching. No `String` objects are created — this is the "zero allocation" philosophy in action.

```java
public QueryType parse(final ByteBuffer buf) {
    final int pos = buf.position();
    final int limit = buf.limit();
    int start = pos;
    while (start < limit && isWhitespace(buf.get(start))) {
        start++;
    }
    // ...
}
```

It skips leading whitespace, then checks if the first keyword matches SELECT, INSERT, UPDATE, or DELETE. All reads use `buf.get(index)` (absolute position reads) which don't move the buffer's internal cursor — so the buffer is untouched after parsing.

```java
private static byte toUpperCase(final byte b) {
    if (b >= 'a' && b <= 'z') return (byte) (b - 32);
    return b;
}
```

Manual case-insensitive comparison at the byte level. `'a'` is ASCII 97, `'A'` is 65, difference is 32. No `String.toUpperCase()` call, no object allocation.

---

## Layer 5: Query Routing

### `QueryRouter.java`

```java
public DatabaseConnection route(final ByteBuffer buf) {
    final QueryType type = parser.parse(buf);
    return switch (type) {
        case READ -> replicaPool.acquire();
        case WRITE, UNKNOWN -> masterPool.acquire();
    };
}
```

**Java concept: `switch` expression** — Java 14+ feature. Unlike the old `switch` statement, this is an expression that returns a value. The `->` syntax means no fall-through (no need for `break`).

SELECT queries go to the replica pool (read scaling), everything else goes to master (where writes must happen). UNKNOWN defaults to master as a safe choice.

The router is stateless and thread-safe — it holds only `final` references to the pools and parser, so any number of worker threads can call `route()` simultaneously.

---

## Layer 6: The Event Loop (NIO I/O Engine)

### `DirectBufferPool.java`

```java
this.pool.set(i, ByteBuffer.allocateDirect(bufferCapacity));
```

**Java concept: `ByteBuffer.allocateDirect()`** — Allocates memory outside the JVM heap, directly in OS memory. This is important because:
1. The OS can do I/O directly to/from this memory without copying (zero-copy)
2. It's not tracked by the garbage collector, so no GC pauses

The pool pre-allocates all buffers at startup. `acquire()` CAS-scans for a non-null slot and atomically swaps it to null (claiming it). `release()` CAS-scans for a null slot and puts the buffer back. Same lock-free pattern as the connection pools.

### `EventLoop.java` — The heart of the system

```java
private final Selector selector;
private final Queue<SocketChannel> pendingRegistrations;
```

**Java concept: `Selector`** — This is the core of Java NIO. A Selector can monitor thousands of socket channels simultaneously on a single thread. Instead of one-thread-per-connection (which doesn't scale), one thread watches all its assigned connections and only acts when data is actually available.

**Java concept: `ConcurrentLinkedQueue`** — A thread-safe queue that uses CAS internally. The acceptor thread puts new channels here, and the event loop thread picks them up. This is the only point where two threads interact for this event loop.

```java
public void run() {
    while (running) {
        selector.select(1000);
        processPendingRegistrations();
        final Set<SelectionKey> selectedKeys = selector.selectedKeys();
        final Iterator<SelectionKey> iter = selectedKeys.iterator();
        while (iter.hasNext()) {
            final SelectionKey key = iter.next();
            iter.remove();
            if (key.isReadable()) {
                handleRead(key);
            }
        }
    }
}
```

This is the infinite event loop:
1. `selector.select(1000)` — block for up to 1 second waiting for any channel to have data ready. Returns immediately if `wakeup()` was called.
2. Process any new channels that were queued by the acceptor thread.
3. Iterate over all channels that have data ready to read.
4. `iter.remove()` — you MUST remove keys from the selected set after processing, or the Selector will report them again next time.

```java
private void handleRead(final SelectionKey key) {
    final ByteBuffer buf = bufferPool.acquire();
    final int bytesRead = channel.read(buf);
    if (bytesRead == -1) {
        key.cancel();
        closeQuietly(channel);
    } else if (bytesRead > 0) {
        buf.flip();
    }
    bufferPool.release(buf);
}
```

Grabs a pre-allocated buffer, reads from the client socket, and `flip()`s the buffer (switches it from write mode to read mode — sets limit to current position and resets position to 0). Right now it just consumes the bytes (Phase 1 proof of concept). In later phases, this is where the buffer would be handed to the `QueryRouter`.

### `EventLoopGroup.java`

```java
private final AtomicInteger nextIndex;

public void dispatch(final SocketChannel channel) {
    final int idx = Math.abs(nextIndex.getAndIncrement() % loops.length);
    loops[idx].registerChannel(channel);
}
```

Manages N event loops (one per CPU core). New client connections are distributed round-robin: connection 0 goes to loop 0, connection 1 to loop 1, etc. This ensures even load distribution across cores.

**Java concept: daemon threads** — `threads[i].setDaemon(true)` means these threads won't prevent the JVM from exiting. When the main thread ends, daemon threads are killed automatically.

---

## Layer 7: The Janitor

### `Janitor.java`

```java
private final ByteBuffer pingBuffer = ByteBuffer.allocateDirect(1);
```

Pre-allocates a tiny 1-byte direct buffer for health probes. Zero allocation during sweeps.

```java
private void sweep() {
    for (int i = 0; i < cap; i++) {
        final DatabaseConnection conn = pool.acquire();
        if (conn == null) break;
        // check age, probe socket
        if (healthy) {
            pool.release(conn);
        } else {
            conn.markStale();
            conn.destroy();
            pool.remove(conn);
            replaced += replaceConnection();
        }
    }
}
```

The janitor acquires connections just like a worker thread would (using the same lock-free `acquire()`). This is elegant — it doesn't need special access. It checks two things:
1. Age — has the connection been alive longer than `maxIdleNanos`?
2. Socket health — does a non-blocking read return -1 (remote closed) or throw an IOException?

Unhealthy connections get the full teardown: mark stale → destroy (close socket) → remove from pool → create a fresh replacement via the factory.

```java
private volatile boolean running;
```

**Java concept: `volatile`** — Guarantees that when one thread writes to this variable, all other threads immediately see the new value. Without `volatile`, the JVM is allowed to cache the value in a CPU register and the janitor thread might never see `running = false`.

---

## Layer 8: The Entry Point

### `PoolerMain.java`

This wires everything together:

1. Parse CLI args (port, pool type, db host/port)
2. Create a `DirectBufferPool` (16,384 buffers × 8KB each = 128MB of off-heap memory)
3. Create master + replica `ConnectionPool` instances (CAS or Ring, chosen at startup)
4. Create `ConnectionFactory` instances for each pool
5. Pre-fill both pools with 128 connections each
6. Create the `QueryRouter` with both pools and the `SqlProtocolParser`
7. Start janitor daemon threads for each pool
8. Start the `EventLoopGroup` (one worker per CPU core)
9. Open a `ServerSocketChannel` on the listen port
10. Enter the accept loop — each new client connection is dispatched to a worker via round-robin

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> { ... }));
```

**Java concept: shutdown hook** — A thread that the JVM runs when the process is being killed (Ctrl+C, SIGTERM, etc.). This ensures graceful cleanup — janitors stop, workers stop.

```java
return switch (type.toLowerCase()) {
    case "ring" -> new RingBufferPool(size);
    case "cas" -> new CasWaitQueue(size);
    default -> { yield new CasWaitQueue(size); }
};
```

**Java concept: `yield`** — In a switch expression, when a case has a block `{ }` instead of a single expression, you use `yield` to return the value from that block.

---

## How It All Flows Together

Here's a request's journey through the system:

```
Client app sends "SELECT * FROM users"
        │
        ▼
ServerSocketChannel.accept()  [PoolerMain accept loop]
        │
        ▼
EventLoopGroup.dispatch()  [round-robin to a worker]
        │
        ▼
EventLoop.registerChannel()  [queued, selector woken up]
        │
        ▼
Selector detects OP_READ  [data arrived from client]
        │
        ▼
handleRead() → DirectBufferPool.acquire() → channel.read(buf)
        │
        ▼
[Future: QueryRouter.route(buf)]
        │
        ▼
SqlProtocolParser.parse(buf) → "SELECT" → QueryType.READ
        │
        ▼
replicaPool.acquire() → CAS flips connection IDLE→BUSY
        │
        ▼
connection.write(buf) → bytes sent to database
        │
        ▼
database responds → connection.read(responseBuf)
        │
        ▼
client.write(responseBuf) → response sent back to app
        │
        ▼
replicaPool.release(connection) → CAS flips BUSY→IDLE
```

Meanwhile, the Janitor is running on its own thread, periodically waking up, scanning for stale connections, and replacing them — completely invisible to the worker threads.

---

## Current State

The event loop currently consumes bytes but is not yet wired to the router (the `handleRead` method has a comment about Phase 2+). The pools, janitor, router, and parser are all fully implemented and ready to be connected.
