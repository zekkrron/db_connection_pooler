package com.dbpooler.pool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * Creates new DatabaseConnection instances by opening non-blocking
 * SocketChannels to a backend database server.
 *
 * Used during pool initialization and by the Janitor when replacing
 * stale connections.
 */
public final class ConnectionFactory {

    private final InetSocketAddress address;

    public ConnectionFactory(final String host, final int port) {
        this.address = new InetSocketAddress(host, port);
    }

    /**
     * Open a new non-blocking connection to the backend database.
     *
     * @return a fresh DatabaseConnection in IDLE state
     * @throws IOException if the connection cannot be established
     */
    public DatabaseConnection create() throws IOException {
        final SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.connect(address);

        // Finish the non-blocking connect
        while (!channel.finishConnect()) {
            Thread.onSpinWait();
        }

        return new DatabaseConnection(channel);
    }

    public InetSocketAddress address() {
        return address;
    }
}
