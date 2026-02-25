package com.dbpooler.pool;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * A sequence counter with cache line padding to eliminate False Sharing.
 *
 * On modern CPUs, a cache line is typically 64 bytes. If two threads
 * write to different variables that happen to sit on the same cache line,
 * the hardware forces a costly cache invalidation ("cache bouncing")
 * even though the variables are logically independent.
 *
 * We pad with 7 dummy longs (56 bytes) on each side of the actual value,
 * guaranteeing the value field sits alone on its own cache line.
 *
 * Uses VarHandle for volatile read/write and CAS — no locks.
 */
public final class PaddedSequence {

    // --- Left padding (56 bytes) ---
    @SuppressWarnings("unused") private long p1, p2, p3, p4, p5, p6, p7;

    // --- The actual sequence value ---
    private volatile long value;

    // --- Right padding (56 bytes) ---
    @SuppressWarnings("unused") private long p9, p10, p11, p12, p13, p14, p15;

    private static final VarHandle VALUE_HANDLE;

    static {
        try {
            VALUE_HANDLE = MethodHandles.lookup()
                    .findVarHandle(PaddedSequence.class, "value", long.class);
        } catch (final ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public PaddedSequence(final long initialValue) {
        this.value = initialValue;
    }

    public long get() {
        return (long) VALUE_HANDLE.getVolatile(this);
    }

    public void set(final long newValue) {
        VALUE_HANDLE.setVolatile(this, newValue);
    }

    public boolean compareAndSet(final long expected, final long newValue) {
        return VALUE_HANDLE.compareAndSet(this, expected, newValue);
    }

    public long getAndIncrement() {
        long current;
        do {
            current = get();
        } while (!compareAndSet(current, current + 1));
        return current;
    }
}
