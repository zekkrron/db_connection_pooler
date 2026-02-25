package com.dbpooler.pool;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * LMAX Disruptor-style circular ring buffer connection pool.
 *
 * Uses independent PaddedSequence counters for producers (release/offer)
 * and consumers (acquire) to eliminate False Sharing. Each sequence sits
 * on its own cache line thanks to the padding in PaddedSequence.
 *
 * The ring size must be a power of 2 so we can use bitwise AND
 * for index wrapping instead of modulo (faster).
 *
 * Lock-free, zero-allocation on the hot path.
 */
public final class RingBufferPool implements ConnectionPool {

    private final AtomicReferenceArray<DatabaseConnection> ring;
    private final int capacity;
    private final int mask;

    // Consumer sequence — where the next acquire() reads from.
    // Padded to its own cache line.
    private final PaddedSequence consumerSeq;

    // Producer sequence — where the next release()/offer() writes to.
    // Padded to its own cache line — no false sharing with consumerSeq.
    private final PaddedSequence producerSeq;

    /**
     * @param requestedCapacity desired pool size (will be rounded up to next power of 2)
     */
    public RingBufferPool(final int requestedCapacity) {
        this.capacity = nextPowerOfTwo(requestedCapacity);
        this.mask = capacity - 1;
        this.ring = new AtomicReferenceArray<>(capacity);
        this.consumerSeq = new PaddedSequence(0);
        this.producerSeq = new PaddedSequence(0);
    }

    @Override
    public DatabaseConnection acquire() {
        // Spin trying to claim the next consumer slot
        for (int spin = 0; spin < capacity; spin++) {
            final long seq = consumerSeq.getAndIncrement();
            final int idx = (int) (seq & mask);

            final DatabaseConnection conn = ring.getAndSet(idx, null);
            if (conn != null && conn.acquire()) {
                return conn;
            }
            // Slot was empty or connection wasn't IDLE — try next
            if (conn != null) {
                // Put it back, it wasn't acquirable (BUSY/STALE/DESTROYED)
                ring.compareAndSet(idx, null, conn);
            }
        }

        return null;
    }

    @Override
    public void release(final DatabaseConnection connection) {
        if (connection == null) return;

        if (!connection.release()) {
            return;
        }

        // Place it back into the ring at the next producer slot
        final long seq = producerSeq.getAndIncrement();
        final int idx = (int) (seq & mask);

        // CAS into the slot — if it's occupied, scan forward
        if (!ring.compareAndSet(idx, null, connection)) {
            for (int i = 1; i < capacity; i++) {
                final int probe = (idx + i) & mask;
                if (ring.compareAndSet(probe, null, connection)) {
                    return;
                }
            }
            // Ring is completely full — shouldn't happen under normal use.
            // Re-acquire the connection so it's not left in IDLE state orphaned.
            connection.acquire();
        }
    }

    @Override
    public boolean offer(final DatabaseConnection connection) {
        if (connection == null) return false;

        final long seq = producerSeq.getAndIncrement();
        final int idx = (int) (seq & mask);

        if (ring.compareAndSet(idx, null, connection)) {
            return true;
        }

        // Primary slot taken, scan for an empty one
        for (int i = 1; i < capacity; i++) {
            final int probe = (idx + i) & mask;
            if (ring.compareAndSet(probe, null, connection)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean remove(final DatabaseConnection connection) {
        if (connection == null) return false;

        for (int i = 0; i < capacity; i++) {
            if (ring.compareAndSet(i, connection, null)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int size() {
        int count = 0;
        for (int i = 0; i < capacity; i++) {
            if (ring.get(i) != null) {
                count++;
            }
        }
        return count;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    private static int nextPowerOfTwo(final int value) {
        if (value <= 1) return 1;
        return Integer.highestOneBit(value - 1) << 1;
    }
}
