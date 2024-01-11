package org.ignis.backend.unix;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Set;

public final class ISocket extends Socket {

    private final SocketChannel channel;
    private boolean bound;
    private boolean inputShutdown;
    private boolean outputShutdown;

    public ISocket() throws IOException {
        this(SocketChannel.open(StandardProtocolFamily.UNIX));
    }

    public ISocket(String address) throws IOException {
        this(SocketChannel.open(StandardProtocolFamily.UNIX));
        connect(UnixDomainSocketAddress.of(address));
    }

    ISocket(SocketChannel channel) throws IOException {
        this.channel = channel;
    }

    @Override
    public void connect(SocketAddress endpoint) throws IOException {
        channel.connect(endpoint);
    }

    @Override
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
        channel.connect(endpoint);
    }

    @Override
    public void bind(SocketAddress bindpoint) throws IOException {
        channel.bind(bindpoint);
        bound = true;
    }

    @Override
    public InetAddress getInetAddress() {
        try {
            return InetAddress.getByName("0.0.0.0");
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InetAddress getLocalAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public int getLocalPort() {
        return 0;
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        try {
            return channel.getRemoteAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
    public SocketChannel getChannel() {
        return channel;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return new ChannelInputStream(channel);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return new ChannelOutputStream(channel);
    }

    @Override
    public void setTcpNoDelay(boolean on) throws SocketException {
    }

    @Override
    public boolean getTcpNoDelay() throws SocketException {
        return true;
    }

    @Override
    public void setSoLinger(boolean on, int linger) throws SocketException {
    }

    @Override
    public int getSoLinger() throws SocketException {
        return -1;
    }

    @Override
    public void sendUrgentData(int data) throws IOException {
    }

    @Override
    public void setOOBInline(boolean on) throws SocketException {
    }

    @Override
    public boolean getOOBInline() throws SocketException {
        return true;
    }

    @Override
    public void setSoTimeout(int timeout) throws SocketException {
    }

    @Override
    public int getSoTimeout() throws SocketException {
        return 0;
    }

    @Override
    public void setSendBufferSize(int size) throws SocketException {
    }

    @Override
    public int getSendBufferSize() throws SocketException {
        return 1024 * 1024;
    }

    @Override
    public void setReceiveBufferSize(int size) throws SocketException {
    }

    @Override
    public int getReceiveBufferSize() throws SocketException {
        return 1024 * 1024;
    }

    @Override
    public void setKeepAlive(boolean on) throws SocketException {
    }

    @Override
    public boolean getKeepAlive() throws SocketException {
        return true;
    }

    @Override
    public void setTrafficClass(int tc) throws SocketException {
    }

    @Override
    public int getTrafficClass() throws SocketException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReuseAddress(boolean on) throws SocketException {
    }

    @Override
    public boolean getReuseAddress() throws SocketException {
        return true;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public void shutdownInput() throws IOException {
        channel.shutdownInput();
        this.inputShutdown = true;
    }

    @Override
    public void shutdownOutput() throws IOException {
        channel.shutdownOutput();
        this.outputShutdown = true;
    }

    @Override
    public boolean isConnected() {
        return channel.isConnected();
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
    public boolean isInputShutdown() {
        return inputShutdown;
    }

    @Override
    public boolean isOutputShutdown() {
        return outputShutdown;
    }

    @Override
    public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
    }

    @Override
    public <T> Socket setOption(SocketOption<T> name, T value) throws IOException {
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

    static class ChannelOutputStream extends OutputStream {

        private final SocketChannel channel;

        ChannelOutputStream(SocketChannel channel) {
            this.channel = channel;
        }

        @Override
        public void write(byte[] b) throws IOException {
            channel.write(ByteBuffer.wrap(b));
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            channel.write(ByteBuffer.wrap(b, off, len));
        }

        @Override
        public void flush() throws IOException {
            super.flush();
        }

        @Override
        public void write(int b) throws IOException {
            this.write(new byte[]{(byte) b});
        }
    }

    static class ChannelInputStream extends InputStream {
        private final SocketChannel channel;

        ChannelInputStream(SocketChannel channel) {
            this.channel = channel;
        }

        @Override
        public int read() throws IOException {
            var buff = new byte[]{0};
            this.read(buff);
            return buff[0];
        }

        @Override
        public int read(byte[] b) throws IOException {
            return channel.read(ByteBuffer.wrap(b));
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return channel.read(ByteBuffer.wrap(b, off, len));
        }
    }
}
