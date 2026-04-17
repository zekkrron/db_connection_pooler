package com.dbpooler.event;

import com.dbpooler.buffer.DirectBufferPool;
import com.dbpooler.pool.DatabaseConnection;
import com.dbpooler.protocol.QueryType;
import com.dbpooler.routing.QueryRouter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A single Worker Thread running an infinite NIO Selector loop.
 *
 * Full data flow per readable event:
 * 1. Read client SQL payload into a pre-allocated direct buffer
 * 2. Parse the query type via the QueryRouter's ProtocolParser
 * 3. Acquire a backend DatabaseConnection from the correct pool (master/replica)
 * 4. Forward the payload to the database
 * 5. Read the database response
 * 6. Write the response back to the client
 *
 * Zero-allocation in the hot path — ByteBuffers come from a pre-allocated pool.
 */
public final class EventLoop implements Runnable {

    private final Selector selector;
    private final DirectBufferPool bufferPool;
    private final QueryRouter router;
    private final Queue<SocketChannel> pendingRegistrations;
    private volatile boolean running;

    public EventLoop(final DirectBufferPool bufferPool, final QueryRouter router) throws IOException {
        this.selector = Selector.open();
        this.bufferPool = bufferPool;
        this.router = router;
        this.pendingRegistrations = new ConcurrentLinkedQueue<>();
        this.running = true;
    }

    /**
     * Enqueue a new client channel for registration on this loop's selector.
     * Called from the acceptor thread — wakes up the selector so it picks up
     * the new channel on the next iteration.
     */
    public void registerChannel(final SocketChannel channel) {
        pendingRegistrations.add(channel);
        selector.wakeup();
    }

    @Override
    public void run() {
        while (running) {
            try {
                selector.select(1000);

                processPendingRegistrations();

                final Set<SelectionKey> selectedKeys = selector.selectedKeys();
                final Iterator<SelectionKey> iter = selectedKeys.iterator();

                while (iter.hasNext()) {
                    final SelectionKey key = iter.next();
                    iter.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isReadable()) {
                        handleRead(key);
                    }
                }
            } catch (final IOException e) {
                System.err.println("[EventLoop] Selector error: " + e.getMessage());
            }
        }

        closeSelector();
    }

    private void processPendingRegistrations() {
        SocketChannel channel;
        while ((channel = pendingRegistrations.poll()) != null) {
            try {
                channel.configureBlocking(false);
                channel.register(selector, SelectionKey.OP_READ);
            } catch (final IOException e) {
                System.err.println("[EventLoop] Failed to register channel: " + e.getMessage());
                closeQuietly(channel);
            }
        }
    }

    /**
     * Full request lifecycle:
     * Client bytes -> parse query type -> route to pool -> forward to DB -> read response -> reply to client.
     */
    private void handleRead(final SelectionKey key) {
        final SocketChannel clientChannel = (SocketChannel) key.channel();
        final ByteBuffer requestBuf = bufferPool.acquire();

        if (requestBuf == null) {
            System.err.println("[EventLoop] Buffer pool exhausted, dropping read");
            return;
        }

        DatabaseConnection dbConn = null;
        QueryType queryType = QueryType.UNKNOWN;
        ByteBuffer responseBuf = null;

        try {
            // --- 1. Read client payload ---
            final int bytesRead = clientChannel.read(requestBuf);

            if (bytesRead == -1) {
                key.cancel();
                closeQuietly(clientChannel);
                return;
            }

            if (bytesRead == 0) {
                return;
            }

            requestBuf.flip();

            // --- 2. Parse query type and route to the correct pool ---
            queryType = router.parseType(requestBuf);
            dbConn = router.acquireFor(queryType);

            if (dbConn == null) {
                System.err.println("[EventLoop] No available connection for " + queryType + ", dropping request");
                return;
            }

            // --- 3. Forward client payload to the database ---
            while (requestBuf.hasRemaining()) {
                dbConn.write(requestBuf);
            }

            // --- 4. Read database response ---
            responseBuf = bufferPool.acquire();
            if (responseBuf == null) {
                System.err.println("[EventLoop] Buffer pool exhausted for response, dropping");
                return;
            }

            final int dbBytes = dbConn.read(responseBuf);

            if (dbBytes > 0) {
                responseBuf.flip();

                // --- 5. Write response back to client ---
                while (responseBuf.hasRemaining()) {
                    clientChannel.write(responseBuf);
                }
            } else if (dbBytes == -1) {
                // Database closed the connection — mark stale so janitor replaces it
                dbConn.markStale();
                dbConn = null;
                key.cancel();
                closeQuietly(clientChannel);
            }

        } catch (final IOException e) {
            key.cancel();
            closeQuietly(clientChannel);
            if (dbConn != null) {
                dbConn.markStale();
                dbConn = null;
            }
        } finally {
            bufferPool.release(requestBuf);
            if (responseBuf != null) {
                bufferPool.release(responseBuf);
            }
            if (dbConn != null) {
                router.releaseFor(dbConn, queryType);
            }
        }
    }

    public void shutdown() {
        running = false;
        selector.wakeup();
    }

    private void closeSelector() {
        try {
            for (final SelectionKey key : selector.keys()) {
                closeQuietly(key.channel());
            }
            selector.close();
        } catch (final IOException e) {
            System.err.println("[EventLoop] Error closing selector: " + e.getMessage());
        }
    }

    private void closeQuietly(final java.nio.channels.Channel channel) {
        try {
            channel.close();
        } catch (final IOException ignored) {
            // intentionally empty
        }
    }
}
