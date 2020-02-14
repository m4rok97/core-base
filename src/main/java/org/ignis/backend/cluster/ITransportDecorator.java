/*
 * Copyright (C) 2020 César Pomar
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
public class ITransportDecorator extends TTransport {

    private TTransport concreteTransport;

    public ITransportDecorator() {
    }

    public ITransportDecorator(TTransport concreteTransport) {
        this.concreteTransport = concreteTransport;
    }

    public TTransport getConcreteTransport() {
        return concreteTransport;
    }

    public void setConcreteTransport(TTransport concreteTransport) {
        this.concreteTransport = concreteTransport;
    }

    @Override
    public boolean isOpen() {
        return concreteTransport.isOpen();
    }

    @Override
    public boolean peek() {
        return concreteTransport.peek();
    }

    @Override
    public void open() throws TTransportException {
        concreteTransport.open();
    }

    @Override
    public void close() {
        concreteTransport.close();
    }

    @Override
    public int read(byte[] arg0, int arg1, int arg2) throws TTransportException {
        return concreteTransport.read(arg0, arg1, arg2);
    }

    @Override
    public int readAll(byte[] buf, int off, int len) throws TTransportException {
        return concreteTransport.readAll(buf, off, len);
    }

    @Override
    public void write(byte[] buf) throws TTransportException {
        concreteTransport.write(buf);
    }

    @Override
    public void write(byte[] arg0, int arg1, int arg2) throws TTransportException {
        concreteTransport.write(arg0, arg1, arg2);
    }

    @Override
    public void flush() throws TTransportException {
        concreteTransport.flush();
    }

    @Override
    public byte[] getBuffer() {
        return concreteTransport.getBuffer();
    }

    @Override
    public int getBufferPosition() {
        return concreteTransport.getBufferPosition();
    }

    @Override
    public int getBytesRemainingInBuffer() {
        return concreteTransport.getBytesRemainingInBuffer();
    }

    @Override
    public void consumeBuffer(int len) {
        concreteTransport.consumeBuffer(len);
    }

}
