package com.dbpooler.protocol;

import java.nio.ByteBuffer;

/**
 * Parses raw bytes from a client connection to determine the query type.
 * Implementations inspect the SQL payload in the ByteBuffer without
 * allocating new objects — the buffer is read in-place.
 *
 * The worker thread passes the raw ByteBuffer here; the parser returns
 * a QueryType so the router knows which pool (master vs replica) to use.
 */
public interface ProtocolParser {

    /**
     * Determine the query type from the raw byte payload.
     * The buffer should be in read mode (flipped).
     * Implementations must not modify the buffer's position.
     *
     * @param buf the client's raw SQL payload
     * @return READ for SELECT queries, WRITE for INSERT/UPDATE/DELETE, UNKNOWN otherwise
     */
    QueryType parse(ByteBuffer buf);
}
