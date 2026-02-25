package com.dbpooler.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Pre-allocated pool of direct (off-heap) ByteBuffers.
 * Zero-allocation on the hot path — buffers are acquired and released
 * via a lock-free CAS scan over a fixed-size array.
 */
public final class DirectBufferPool {

    private final AtomicReferenceArray<ByteBuffer> pool;
    private final int bufferCapacity;

    public DirectBufferPool(final int poolSize, final int bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
        this.pool = new AtomicReferenceArray<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            this.pool.set(i, ByteBuffer.allocateDirect(bufferCapacity));
        }
    }

    /**
     * Acquires a direct ByteBuffer from the pool.
     * Lock-free: CAS spins over slots until one is claimed.
     *
     * @return a cleared ByteBuffer, or null if the pool is exhausted
     */
    public ByteBuffer acquire() {
        final int length = pool.length();
        for (int i = 0; i < length; i++) {
            final ByteBuffer buf = pool.getAndSet(i, null);
            if (buf != null) {
                buf.clear();
                return buf;
            }
        }
        return null;
    }

    /**
     * Returns a ByteBuffer back to the pool.
     *
     * @param buf the buffer to release
     */
    public void release(final ByteBuffer buf) {
        if (buf == null) return;
        buf.clear();
        final int length = pool.length();
        for (int i = 0; i < length; i++) {
            if (pool.compareAndSet(i, null, buf)) {
                return;
            }
        }
        // Pool is full — buffer is discarded (shouldn't happen under normal use)
    }

    public int bufferCapacity() {
        return bufferCapacity;
    }
}
