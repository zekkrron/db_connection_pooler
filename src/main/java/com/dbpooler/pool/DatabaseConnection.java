package com.dbpooler.pool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wraps a raw SocketChannel to a backend database server.
 * Maintains a strict atomic state machine: IDLE -> BUSY -> STALE -> DESTROYED.
 *
 * State transitions are thread-safe via CAS on an AtomicReference.
 */
public final class DatabaseConnection {

    public enum State {
        IDLE, BUSY, STALE, DESTROYED
    }

    private final SocketChannel channel;
    private final AtomicReference<State> state;
    private final long createdAt;

    public DatabaseConnection(final SocketChannel channel) {
        this.channel = channel;
        this.state = new AtomicReference<>(State.IDLE);
        this.createdAt = System.nanoTime();
    }

    /**
     * Attempt to acquire this connection for use.
     * Only succeeds if current state is IDLE — atomically transitions to BUSY.
     *
     * @return true if successfully acquired
     */
    public boolean acquire() {
        return state.compareAndSet(State.IDLE, State.BUSY);
    }

    /**
     * Release this connection back to IDLE after use.
     * Only succeeds if current state is BUSY.
     *
     * @return true if successfully released
     */
    public boolean release() {
        return state.compareAndSet(State.BUSY, State.IDLE);
    }

    /**
     * Mark this connection as stale (e.g., failed health check).
     * Can transition from IDLE or BUSY.
     *
     * @return true if successfully marked stale
     */
    public boolean markStale() {
        State current;
        do {
            current = state.get();
            if (current == State.DESTROYED) {
                return false;
            }
        } while (!state.compareAndSet(current, State.STALE));
        return true;
    }

    /**
     * Destroy this connection — closes the underlying socket.
     * Terminal state, no further transitions allowed.
     */
    public void destroy() {
        final State prev = state.getAndSet(State.DESTROYED);
        if (prev != State.DESTROYED) {
            try {
                channel.close();
            } catch (final IOException ignored) {
                // best-effort close
            }
        }
    }

    /**
     * Write a ByteBuffer to the backend database socket.
     *
     * @param buf the buffer to write (must be flipped/ready for reading)
     * @return bytes written
     * @throws IOException if the write fails
     */
    public int write(final ByteBuffer buf) throws IOException {
        return channel.write(buf);
    }

    /**
     * Read from the backend database socket into the given buffer.
     *
     * @param buf the buffer to read into
     * @return bytes read, or -1 if the connection was closed
     * @throws IOException if the read fails
     */
    public int read(final ByteBuffer buf) throws IOException {
        return channel.read(buf);
    }

    public State state() {
        return state.get();
    }

    public SocketChannel channel() {
        return channel;
    }

    public long createdAt() {
        return createdAt;
    }

    public boolean isUsable() {
        return state.get() == State.IDLE;
    }
}
