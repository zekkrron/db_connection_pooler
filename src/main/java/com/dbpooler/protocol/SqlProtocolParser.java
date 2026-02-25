package com.dbpooler.protocol;

import java.nio.ByteBuffer;

/**
 * Zero-allocation SQL parser that inspects the first keyword in a raw ByteBuffer
 * to classify the query as READ or WRITE.
 *
 * Reads bytes directly from the buffer without creating String objects.
 * The buffer's position is preserved (we use absolute gets).
 */
public final class SqlProtocolParser implements ProtocolParser {

    // Pre-computed uppercase byte sequences for matching
    private static final byte[] SELECT = {'S', 'E', 'L', 'E', 'C', 'T'};
    private static final byte[] INSERT = {'I', 'N', 'S', 'E', 'R', 'T'};
    private static final byte[] UPDATE = {'U', 'P', 'D', 'A', 'T', 'E'};
    private static final byte[] DELETE = {'D', 'E', 'L', 'E', 'T', 'E'};

    @Override
    public QueryType parse(final ByteBuffer buf) {
        final int pos = buf.position();
        final int limit = buf.limit();

        // Skip leading whitespace
        int start = pos;
        while (start < limit && isWhitespace(buf.get(start))) {
            start++;
        }

        final int remaining = limit - start;

        if (remaining >= SELECT.length && matchesKeyword(buf, start, SELECT)) {
            return QueryType.READ;
        }
        if (remaining >= INSERT.length && matchesKeyword(buf, start, INSERT)) {
            return QueryType.WRITE;
        }
        if (remaining >= UPDATE.length && matchesKeyword(buf, start, UPDATE)) {
            return QueryType.WRITE;
        }
        if (remaining >= DELETE.length && matchesKeyword(buf, start, DELETE)) {
            return QueryType.WRITE;
        }

        return QueryType.UNKNOWN;
    }

    /**
     * Case-insensitive match of a keyword at the given buffer offset.
     * Uses absolute gets — does not modify buffer position.
     */
    private static boolean matchesKeyword(final ByteBuffer buf, final int offset, final byte[] keyword) {
        for (int i = 0; i < keyword.length; i++) {
            final byte b = buf.get(offset + i);
            final byte upper = toUpperCase(b);
            if (upper != keyword[i]) {
                return false;
            }
        }
        return true;
    }

    private static byte toUpperCase(final byte b) {
        if (b >= 'a' && b <= 'z') {
            return (byte) (b - 32);
        }
        return b;
    }

    private static boolean isWhitespace(final byte b) {
        return b == ' ' || b == '\t' || b == '\n' || b == '\r';
    }
}
