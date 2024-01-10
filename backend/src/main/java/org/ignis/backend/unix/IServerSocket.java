package org.ignis.backend.unix;

import java.io.IOException;
import java.net.*;
import java.nio.channels.ServerSocketChannel;
import java.util.Set;

public final class IServerSocket extends ServerSocket {

    private final ServerSocketChannel channel;
    private boolean bound;

    public IServerSocket() throws IOException {
        channel = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
    }

    public IServerSocket(String address) throws IOException {
        this();
        channel.bind(UnixDomainSocketAddress.of(address));
        bound = true;
    }

    @Override
    public Socket accept() throws IOException {
        return new ISocket(channel.accept());
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public ServerSocketChannel getChannel() {
        return channel;
    }

    @Override
    public boolean isBound() {
        return bound;
    }

    @Override
    public boolean isClosed() {
        return !channel.isOpen();
    }

    @Override
    public void bind(SocketAddress endpoint) throws IOException {
        channel.bind(endpoint);
        bound = true;
    }

    @Override
    public void bind(SocketAddress endpoint, int backlog) throws IOException {
        channel.bind(endpoint, backlog);
        bound = true;
    }

    @Override
    public InetAddress getInetAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLocalPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        try {
            return channel.getLocalAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setSoTimeout(int timeout) throws SocketException {
    }

    @Override
    public int getSoTimeout() throws IOException {
        return 0;
    }

    @Override
    public void setReuseAddress(boolean on) throws SocketException {
    }

    @Override
    public boolean getReuseAddress() throws SocketException {
        return true;
    }

    @Override
    public void setReceiveBufferSize(int size) throws SocketException {
    }

    @Override
    public int getReceiveBufferSize() throws SocketException {
        return 1024 * 1024;
    }

    @Override
    public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
    }

    @Override
    public <T> ServerSocket setOption(SocketOption<T> name, T value) throws IOException {
        channel.setOption(name, value);
        return this;
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        return channel.getOption(name);
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        return channel.supportedOptions();
    }
}
