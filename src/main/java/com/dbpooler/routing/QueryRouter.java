package com.dbpooler.routing;

import com.dbpooler.pool.ConnectionPool;
import com.dbpooler.pool.DatabaseConnection;
import com.dbpooler.protocol.ProtocolParser;
import com.dbpooler.protocol.QueryType;

import java.nio.ByteBuffer;

/**
 * Routes a client's SQL payload to the correct backend pool.
 *
 * SELECT -> replica pool
 * INSERT/UPDATE/DELETE -> master pool
 * UNKNOWN -> master pool (safe default)
 *
 * Stateless and thread-safe — worker threads each call route() independently.
 */
public final class QueryRouter {

    private final ConnectionPool masterPool;
    private final ConnectionPool replicaPool;
    private final ProtocolParser parser;

    public QueryRouter(final ConnectionPool masterPool,
                       final ConnectionPool replicaPool,
                       final ProtocolParser parser) {
        this.masterPool = masterPool;
        this.replicaPool = replicaPool;
        this.parser = parser;
    }

    /**
     * Parse the query type from the buffer and acquire a connection
     * from the appropriate pool.
     *
     * @param buf the client's raw SQL payload (flipped, ready for reading)
     * @return a DatabaseConnection in BUSY state, or null if pool is exhausted
     */
    public DatabaseConnection route(final ByteBuffer buf) {
        final QueryType type = parser.parse(buf);

        return switch (type) {
            case READ -> replicaPool.acquire();
            case WRITE, UNKNOWN -> masterPool.acquire();
        };
    }

    /**
     * Determine the query type from the raw byte payload without acquiring a connection.
     * Used by the EventLoop to separate parsing from acquisition.
     *
     * @param buf the client's raw SQL payload (flipped, ready for reading)
     * @return the classified QueryType
     */
    public QueryType parseType(final ByteBuffer buf) {
        return parser.parse(buf);
    }

    /**
     * Acquire a connection from the pool matching the given query type.
     *
     * @param type the query classification
     * @return a DatabaseConnection in BUSY state, or null if pool is exhausted
     */
    public DatabaseConnection acquireFor(final QueryType type) {
        return switch (type) {
            case READ -> replicaPool.acquire();
            case WRITE, UNKNOWN -> masterPool.acquire();
        };
    }

    /**
     * Release a connection back to whichever pool matches the query type.
     *
     * @param connection the connection to release
     * @param type       the query type that determined the pool
     */
    public void releaseFor(final DatabaseConnection connection, final QueryType type) {
        switch (type) {
            case READ -> replicaPool.release(connection);
            case WRITE, UNKNOWN -> masterPool.release(connection);
        }
    }

    public ConnectionPool masterPool() {
        return masterPool;
    }

    public ConnectionPool replicaPool() {
        return replicaPool;
    }
}
