package com.dbpooler;

import com.dbpooler.buffer.DirectBufferPool;
import com.dbpooler.event.EventLoopGroup;
import com.dbpooler.janitor.Janitor;
import com.dbpooler.pool.CasWaitQueue;
import com.dbpooler.pool.ConnectionFactory;
import com.dbpooler.pool.ConnectionPool;
import com.dbpooler.pool.RingBufferPool;
import com.dbpooler.protocol.SqlProtocolParser;
import com.dbpooler.routing.QueryRouter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * Entry point for the connection pooler.
 *
 * Usage:
 *   java com.dbpooler.PoolerMain [listenPort] [poolType] [dbHost] [dbPort]
 *
 *   poolType: "cas" (default) or "ring"
 *
 * Binds a ServerSocketChannel, creates master + replica pools,
 * starts the janitor, and dispatches connections to worker threads.
 */
public final class PoolerMain {

    private static final int DEFAULT_PORT = 3307;
    private static final int BUFFER_POOL_SIZE = 16384;
    private static final int BUFFER_CAPACITY = 8192;
    private static final int CONNECTION_POOL_SIZE = 128;
    private static final long MAX_IDLE_SECONDS = 300;
    private static final long JANITOR_INTERVAL_SECONDS = 30;

    public static void main(final String[] args) throws IOException {
        final int port = argInt(args, 0, DEFAULT_PORT);
        final String poolType = argStr(args, 1, "cas");
        final String dbHost = argStr(args, 2, "127.0.0.1");
        final int dbPort = argInt(args, 3, 5432);
        final int workerCount = Runtime.getRuntime().availableProcessors();

        // --- Buffer pool (shared across all workers) ---
        final DirectBufferPool bufferPool = new DirectBufferPool(BUFFER_POOL_SIZE, BUFFER_CAPACITY);

        // --- Connection pools (master + replica) ---
        final ConnectionPool masterPool = createPool(poolType, CONNECTION_POOL_SIZE);
        final ConnectionPool replicaPool = createPool(poolType, CONNECTION_POOL_SIZE);

        // --- Connection factories ---
        final ConnectionFactory masterFactory = new ConnectionFactory(dbHost, dbPort);
        final ConnectionFactory replicaFactory = new ConnectionFactory(dbHost, dbPort);

        // --- Pre-fill pools ---
        fillPool(masterPool, masterFactory, CONNECTION_POOL_SIZE);
        fillPool(replicaPool, replicaFactory, CONNECTION_POOL_SIZE);

        // --- Query router ---
        final QueryRouter router = new QueryRouter(masterPool, replicaPool, new SqlProtocolParser());

        // --- Janitor threads (one per pool) ---
        final Janitor masterJanitor = new Janitor(masterPool, masterFactory, MAX_IDLE_SECONDS, JANITOR_INTERVAL_SECONDS);
        final Janitor replicaJanitor = new Janitor(replicaPool, replicaFactory, MAX_IDLE_SECONDS, JANITOR_INTERVAL_SECONDS);

        final Thread masterJanitorThread = new Thread(masterJanitor, "janitor-master");
        masterJanitorThread.setDaemon(true);
        masterJanitorThread.start();

        final Thread replicaJanitorThread = new Thread(replicaJanitor, "janitor-replica");
        replicaJanitorThread.setDaemon(true);
        replicaJanitorThread.start();

        // --- Worker threads ---
        final EventLoopGroup group = new EventLoopGroup(workerCount, bufferPool);
        group.start();

        // --- Accept loop ---
        try (final ServerSocketChannel server = ServerSocketChannel.open()) {
            server.bind(new InetSocketAddress(port));
            server.configureBlocking(true);

            System.out.println("[PoolerMain] Listening on port " + port
                    + " | pool: " + poolType
                    + " | workers: " + workerCount
                    + " | backend: " + dbHost + ":" + dbPort);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[PoolerMain] Shutting down...");
                masterJanitor.shutdown();
                replicaJanitor.shutdown();
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

    private static ConnectionPool createPool(final String type, final int size) {
        return switch (type.toLowerCase()) {
            case "ring" -> new RingBufferPool(size);
            case "cas" -> new CasWaitQueue(size);
            default -> {
                System.out.println("[PoolerMain] Unknown pool type '" + type + "', defaulting to CAS");
                yield new CasWaitQueue(size);
            }
        };
    }

    private static void fillPool(final ConnectionPool pool, final ConnectionFactory factory, final int count) {
        int created = 0;
        for (int i = 0; i < count; i++) {
            try {
                if (pool.offer(factory.create())) {
                    created++;
                }
            } catch (final IOException e) {
                System.err.println("[PoolerMain] Failed to pre-fill connection: " + e.getMessage());
                break;
            }
        }
        System.out.println("[PoolerMain] Pre-filled pool with " + created + " connections");
    }

    private static int argInt(final String[] args, final int index, final int defaultValue) {
        if (args.length > index) {
            try {
                return Integer.parseInt(args[index]);
            } catch (final NumberFormatException ignored) {
                // fall through
            }
        }
        return defaultValue;
    }

    private static String argStr(final String[] args, final int index, final String defaultValue) {
        return args.length > index ? args[index] : defaultValue;
    }
}
