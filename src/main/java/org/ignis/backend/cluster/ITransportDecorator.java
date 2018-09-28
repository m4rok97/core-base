/*
 * Copyright (C) 2018 
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.ignis.backend.cluster;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 *
 * @author César Pomar
 */
public class ITransportDecorator extends TTransport{
    
    private TTransport transport;

    public ITransportDecorator(TTransport transport) {
        this.transport = transport;
    }

    public TTransport getTransport() {
        return transport;
    }

    public void setTransport(TTransport transport) {
        this.transport = transport;
    }

    @Override
    public boolean isOpen() {
        return transport.isOpen();
    }

    @Override
    public boolean peek() {
        return transport.peek();
    }

    @Override
    public void open() throws TTransportException {
        transport.open();
    }

    @Override
    public void close() {
        transport.close();
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        return transport.read(buf, off, len);
    }

    @Override
    public int readAll(byte[] buf, int off, int len) throws TTransportException {
        return transport.readAll(buf, off, len);
    }

    @Override
    public void write(byte[] buf) throws TTransportException {
        transport.write(buf);
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        transport.write(buf, off, len);
    }

    @Override
    public void flush() throws TTransportException {
        transport.flush();
    }

    @Override
    public byte[] getBuffer() {
        return transport.getBuffer();
    }

    @Override
    public int getBufferPosition() {
        return transport.getBufferPosition();
    }

    @Override
    public int getBytesRemainingInBuffer() {
        return transport.getBytesRemainingInBuffer();
    }

    @Override
    public void consumeBuffer(int len) {
        transport.consumeBuffer(len);
    }   
    
}
