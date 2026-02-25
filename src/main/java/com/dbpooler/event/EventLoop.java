package com.dbpooler.event;

import com.dbpooler.buffer.DirectBufferPool;

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
 * Handles readable events on client connections assigned to it.
 * Zero-allocation in the hot path — ByteBuffers come from a pre-allocated pool.
 */
public final class EventLoop implements Runnable {

    private final Selector selector;
    private final DirectBufferPool bufferPool;
    private final Queue<SocketChannel> pendingRegistrations;
    private volatile boolean running;

    public EventLoop(final DirectBufferPool bufferPool) throws IOException {
        this.selector = Selector.open();
        this.bufferPool = bufferPool;
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

    private void handleRead(final SelectionKey key) {
        final SocketChannel channel = (SocketChannel) key.channel();
        final ByteBuffer buf = bufferPool.acquire();

        if (buf == null) {
            System.err.println("[EventLoop] Buffer pool exhausted, dropping read");
            return;
        }

        try {
            final int bytesRead = channel.read(buf);

            if (bytesRead == -1) {
                // Client disconnected
                key.cancel();
                closeQuietly(channel);
            } else if (bytesRead > 0) {
                buf.flip();
                // Phase 1: just consume the bytes to prove the loop works.
                // Phase 2+ will hand this buffer to the ProtocolParser.
            }
        } catch (final IOException e) {
            key.cancel();
            closeQuietly(channel);
        } finally {
            bufferPool.release(buf);
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
