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

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TZlibTransport;
import org.ignis.backend.allocator.IContainerStub;
import org.ignis.backend.allocator.IExecutorStub;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.properties.IPropsKeys;
import org.ignis.rpc.manager.IFileManager;
import org.ignis.rpc.manager.IRegisterManager;
import org.ignis.rpc.manager.IServerManager;

/**
 *
 * @author César Pomar
 */
public final class IContainer {

    private final long id;
    private final IContainerStub stub;
    private final ITransportDecorator transport;
    private final TProtocol protocol;
    private final IServerManager.Iface serverManager;
    private final IRegisterManager.Iface registerManager;
    private final IFileManager.Iface fileManager;

    public IContainer(long id, IContainerStub stub) throws IgnisException {
        this.id = id;
        this.stub = stub;
        this.transport = new ITransportDecorator(null);// null before connect
        this.protocol = new TCompactProtocol(new TZlibTransport(transport,
                stub.getProperties().getInteger(IPropsKeys.MANAGER_RPC_COMPRESSION)));
        this.serverManager = new IServerManager.Client(new TMultiplexedProtocol(protocol, "server"));
        this.registerManager = new IRegisterManager.Client(new TMultiplexedProtocol(protocol, "register"));
        this.fileManager = new IFileManager.Client(new TMultiplexedProtocol(protocol, "file"));
    }

    public long getId() {
        return id;
    }

    public void connect() throws IgnisException {
        TSocket socket = new TSocket(
                stub.getHost(), stub.getHostPort(stub.getProperties().getInteger(IPropsKeys.MANAGER_RPC_PORT)));
        transport.setTransport(new TZlibTransport(
                socket, stub.getProperties().getInteger(IPropsKeys.MANAGER_RPC_COMPRESSION)));

        for (int i = 0; i < 10; i++) {
            try {
                socket.open();
                break;
            } catch (TTransportException ex) {
                if (i == 9) {
                    throw new IgnisException(ex.getMessage(), ex);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex1) {
                    throw new IgnisException(ex.getMessage(), ex);
                }
            }
        }
    }

    public IExecutor createExecutor(long job, IExecutorStub stub) {
        return new IExecutor(job, this, stub, protocol);
    }

    public IServerManager.Iface getServerManager() {
        return serverManager;
    }

    public IRegisterManager.Iface getRegisterManager() {
        return registerManager;
    }

    public IFileManager.Iface getFileManager() {
        return fileManager;
    }

    public IContainerStub getStub() {
        return stub;
    }

    public IProperties getProperties() {
        return stub.getProperties();
    }

    public String getHost() {
        return stub.getHost();
    }

    public int getExposePort(int port) {
        return stub.getHostPort(port);
    }

}
