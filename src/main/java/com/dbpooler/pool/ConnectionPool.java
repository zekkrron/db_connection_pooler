package com.dbpooler.pool;

/**
 * Strategy interface for connection pooling.
 * Worker threads program against this — the concrete implementation
 * (CasWaitQueue or RingBufferPool) is chosen at startup.
 */
public interface ConnectionPool {

    /**
     * Acquire an idle database connection from the pool.
     * Must be lock-free on the hot path.
     *
     * @return a DatabaseConnection in BUSY state, or null if none available
     */
    DatabaseConnection acquire();

    /**
     * Return a connection back to the pool after use.
     * Transitions the connection from BUSY back to IDLE.
     *
     * @param connection the connection to release
     */
    void release(DatabaseConnection connection);

    /**
     * Add a fresh connection to the pool (used during init or replacement).
     *
     * @param connection the new connection to add
     * @return true if successfully added
     */
    boolean offer(DatabaseConnection connection);

    /**
     * Remove a connection from the pool (used by the Janitor for cleanup).
     *
     * @param connection the connection to remove
     * @return true if successfully removed
     */
    boolean remove(DatabaseConnection connection);

    /**
     * @return current number of connections in the pool (all states)
     */
    int size();

    /**
     * @return maximum capacity of this pool
     */
    int capacity();
}
