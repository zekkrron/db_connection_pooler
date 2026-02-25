package com.dbpooler.event;

import com.dbpooler.buffer.DirectBufferPool;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages a fixed number of EventLoop worker threads.
 * Distributes incoming connections across loops via round-robin.
 */
public final class EventLoopGroup {

    private final EventLoop[] loops;
    private final Thread[] threads;
    private final AtomicInteger nextIndex;

    public EventLoopGroup(final int threadCount, final DirectBufferPool bufferPool) throws IOException {
        this.loops = new EventLoop[threadCount];
        this.threads = new Thread[threadCount];
        this.nextIndex = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            loops[i] = new EventLoop(bufferPool);
            threads[i] = new Thread(loops[i], "worker-" + i);
            threads[i].setDaemon(true);
        }
    }

    public void start() {
        for (final Thread t : threads) {
            t.start();
        }
        System.out.println("[EventLoopGroup] Started " + threads.length + " worker threads");
    }

    /**
     * Round-robin assignment of a new client channel to a worker loop.
     */
    public void dispatch(final SocketChannel channel) {
        final int idx = Math.abs(nextIndex.getAndIncrement() % loops.length);
        loops[idx].registerChannel(channel);
    }

    public void shutdown() {
        for (final EventLoop loop : loops) {
            loop.shutdown();
        }
        for (final Thread t : threads) {
            try {
                t.join(2000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("[EventLoopGroup] All worker threads stopped");
    }

    public int threadCount() {
        return loops.length;
    }
}
