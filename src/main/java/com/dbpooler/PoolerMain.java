package com.dbpooler;

import com.dbpooler.buffer.DirectBufferPool;
import com.dbpooler.event.EventLoopGroup;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * Entry point. Binds a ServerSocketChannel on the configured port,
 * accepts incoming client connections, and dispatches them to the
 * EventLoopGroup's worker threads via round-robin.
 *
 * The accept loop runs on the main thread — worker threads handle all reads.
 */
public final class PoolerMain {

    private static final int DEFAULT_PORT = 5432;
    private static final int BUFFER_POOL_SIZE = 16384;
    private static final int BUFFER_CAPACITY = 8192;

    public static void main(final String[] args) throws IOException {
        final int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        final int workerCount = Runtime.getRuntime().availableProcessors();

        final DirectBufferPool bufferPool = new DirectBufferPool(BUFFER_POOL_SIZE, BUFFER_CAPACITY);
        final EventLoopGroup group = new EventLoopGroup(workerCount, bufferPool);
        group.start();

        try (final ServerSocketChannel server = ServerSocketChannel.open()) {
            server.bind(new InetSocketAddress(port));
            server.configureBlocking(true); // accept() is the only blocking call — by design

            System.out.println("[PoolerMain] Listening on port " + port
                    + " with " + workerCount + " worker threads");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[PoolerMain] Shutting down...");
                group.shutdown();
            }));

            while (true) {
                final SocketChannel client = server.accept();
                if (client != null) {
                    group.dispatch(client);
                }
            }
        }
    }
}
