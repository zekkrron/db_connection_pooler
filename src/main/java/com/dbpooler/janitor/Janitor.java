package com.dbpooler.janitor;

import com.dbpooler.pool.ConnectionFactory;
import com.dbpooler.pool.ConnectionPool;
import com.dbpooler.pool.DatabaseConnection;
import com.dbpooler.pool.DatabaseConnection.State;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Background daemon thread that maintains pool health.
 *
 * Periodically:
 * 1. Scans all connections in the pool
 * 2. Sends a lightweight ping to each IDLE connection
 * 3. Marks failed or expired connections as STALE
 * 4. Destroys STALE connections and replaces them with fresh ones
 *
 * Runs on a single isolated thread — never touches the worker hot path.
 */
public final class Janitor implements Runnable {

    private final ConnectionPool pool;
    private final ConnectionFactory factory;
    private final long maxIdleNanos;
    private final long intervalMillis;
    private final ByteBuffer pingBuffer;
    private volatile boolean running;

    /**
     * @param pool           the connection pool to maintain
     * @param factory        factory for creating replacement connections
     * @param maxIdleSeconds max time a connection can sit idle before being considered stale
     * @param intervalSeconds how often the janitor runs its sweep (in seconds)
     */
    public Janitor(final ConnectionPool pool,
                   final ConnectionFactory factory,
                   final long maxIdleSeconds,
                   final long intervalSeconds) {
        this.pool = pool;
        this.factory = factory;
        this.maxIdleNanos = maxIdleSeconds * 1_000_000_000L;
        this.intervalMillis = intervalSeconds * 1000L;
        // Pre-allocate a small direct buffer for ping probes
        this.pingBuffer = ByteBuffer.allocateDirect(1);
        this.running = true;
    }

    @Override
    public void run() {
        System.out.println("[Janitor] Started — sweep interval: " + (intervalMillis / 1000) + "s");

        while (running) {
            try {
                Thread.sleep(intervalMillis);
                sweep();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        System.out.println("[Janitor] Stopped");
    }

    /**
     * One full sweep of the pool.
     * Checks each connection's health and age, replaces bad ones.
     */
    private void sweep() {
        int checked = 0;
        int replaced = 0;

        // We scan up to pool capacity. Connections in BUSY state are skipped.
        final int cap = pool.capacity();

        for (int i = 0; i < cap; i++) {
            // Try to acquire a connection for health checking
            final DatabaseConnection conn = pool.acquire();
            if (conn == null) {
                // No more idle connections to check
                break;
            }

            checked++;

            boolean healthy = true;

            // Check age
            final long age = System.nanoTime() - conn.createdAt();
            if (age > maxIdleNanos) {
                healthy = false;
            }

            // Probe the socket — try a zero-byte read to detect closed connections
            if (healthy) {
                healthy = probe(conn);
            }

            if (healthy) {
                // Put it back
                pool.release(conn);
            } else {
                // Mark stale, destroy, remove, and replace
                conn.markStale();
                conn.destroy();
                pool.remove(conn);
                replaced += replaceConnection();
            }
        }

        if (checked > 0) {
            System.out.println("[Janitor] Sweep complete — checked: " + checked
                    + ", replaced: " + replaced);
        }
    }

    /**
     * Probe a connection by attempting a non-blocking read.
     * If the remote end has closed, read() returns -1.
     * If the socket is broken, an IOException is thrown.
     *
     * @return true if the connection appears healthy
     */
    private boolean probe(final DatabaseConnection conn) {
        try {
            pingBuffer.clear();
            final int result = conn.read(pingBuffer);
            // -1 means remote closed the connection
            return result != -1;
        } catch (final IOException e) {
            return false;
        }
    }

    /**
     * Create a fresh connection and offer it to the pool.
     *
     * @return 1 if replacement succeeded, 0 otherwise
     */
    private int replaceConnection() {
        try {
            final DatabaseConnection fresh = factory.create();
            if (pool.offer(fresh)) {
                return 1;
            } else {
                // Pool rejected it (full) — destroy immediately
                fresh.destroy();
            }
        } catch (final IOException e) {
            System.err.println("[Janitor] Failed to create replacement connection: " + e.getMessage());
        }
        return 0;
    }

    public void shutdown() {
        running = false;
    }
}
