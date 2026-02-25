package com.dbpooler.pool;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Lock-free connection pool using hardware Compare-And-Swap.
 *
 * Internally a fixed-size array of DatabaseConnection slots.
 * Threads compete for a shared "next available" scan pointer.
 * acquire() CAS-spins over slots trying to claim an IDLE connection.
 * release() CAS-spins to find an empty slot to return the connection to.
 *
 * Zero locks, zero allocation on the hot path.
 */
public final class CasWaitQueue implements ConnectionPool {

    private final AtomicReferenceArray<DatabaseConnection> slots;
    private final AtomicInteger count;
    private final int capacity;

    // Scan pointer — threads start their search from here and wrap around.
    // Reduces contention vs always starting at index 0.
    private final AtomicInteger scanIndex;

    public CasWaitQueue(final int capacity) {
        this.capacity = capacity;
        this.slots = new AtomicReferenceArray<>(capacity);
        this.count = new AtomicInteger(0);
        this.scanIndex = new AtomicInteger(0);
    }

    @Override
    public DatabaseConnection acquire() {
        final int start = Math.abs(scanIndex.getAndIncrement() % capacity);

        for (int i = 0; i < capacity; i++) {
            final int idx = (start + i) % capacity;
            final DatabaseConnection conn = slots.get(idx);

            if (conn != null && conn.acquire()) {
                // Connection was IDLE, now BUSY — we own it.
                return conn;
            }
        }

        // Full scan, nothing available.
        return null;
    }

    @Override
    public void release(final DatabaseConnection connection) {
        if (connection == null) return;

        // Transition BUSY -> IDLE
        if (!connection.release()) {
            // Connection wasn't in BUSY state — something is wrong, don't return it.
            return;
        }

        // Connection is now IDLE and still sitting in its slot.
        // No slot manipulation needed — acquire() will find it by scanning.
    }

    @Override
    public boolean offer(final DatabaseConnection connection) {
        if (connection == null) return false;

        for (int i = 0; i < capacity; i++) {
            if (slots.compareAndSet(i, null, connection)) {
                count.incrementAndGet();
                return true;
            }
        }

        // Pool is full.
        return false;
    }

    @Override
    public boolean remove(final DatabaseConnection connection) {
        if (connection == null) return false;

        for (int i = 0; i < capacity; i++) {
            if (slots.compareAndSet(i, connection, null)) {
                count.decrementAndGet();
                return true;
            }
        }

        return false;
    }

    @Override
    public int size() {
        return count.get();
    }

    @Override
    public int capacity() {
        return capacity;
    }
}
