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
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TZlibTransport;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.executor.ICacheContextModule;
import org.ignis.rpc.executor.IExecutorServerModule;
import org.ignis.rpc.executor.IGeneralActionModule;
import org.ignis.rpc.executor.IGeneralModule;
import org.ignis.rpc.executor.IIOModule;
import org.ignis.rpc.executor.IMathModule;

/**
 *
 * @author César Pomar
 */
public final class IExecutor {

    private final long worker;
    private final int port;
    private final IContainer container;
    private final TTransport transport;
    private final TProtocol protocol;
    private final IExecutorServerModule.Iface executorServerModule;
    private final IGeneralModule.Iface generalModule;
    private final IGeneralActionModule.Iface generalActionModule;
    private final IMathModule.Iface mathModule;
    private final IIOModule.Iface ioModule;
    private final ICacheContextModule.Iface cacheContextModule;
    private int pid;

    public IExecutor(long worker, IContainer container, int port) {
        this.worker = worker;
        this.container = container;
        this.port = port;
        this.transport = new TSocket("localhost", port);
        this.protocol = new TCompactProtocol(new TZlibTransport(transport,
                container.getProperties().getInteger(IKeys.MANAGER_RPC_COMPRESSION)));
        executorServerModule = new IExecutorServerModule.Client(new TMultiplexedProtocol(protocol, "IExecutorServer"));
        generalModule = new IGeneralModule.Client(new TMultiplexedProtocol(protocol, "IGeneral"));
        generalActionModule = new IGeneralActionModule.Client(new TMultiplexedProtocol(protocol, "IGeneralAction"));
        mathModule = new IMathModule.Client(new TMultiplexedProtocol(protocol, "IMath"));
        ioModule = new IIOModule.Client(new TMultiplexedProtocol(protocol, "IIO"));
        cacheContextModule = new ICacheContextModule.Client(new TMultiplexedProtocol(protocol, "ICacheContext"));
    }

    public long getWorker() {
        return worker;
    }

    public long getId() {
        if (container == null) {
            return 0;
        }
        return container.getId();
    }

    public IContainer getContainer() {
        return container;
    }

    public int getPort() {
        return port;
    }

    public IProperties getProperties() {
        if (container == null) {
            return null;
        }
        return container.getProperties();
    }

    public TTransport getTransport() {
        return transport;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public IExecutorServerModule.Iface getExecutorServerModule() {
        return executorServerModule;
    }

    public IGeneralModule.Iface getGeneralModule() {
        return generalModule;
    }

    public IGeneralActionModule.Iface getGeneralActionModule() {
        return generalActionModule;
    }

    public IMathModule.Iface getMathModule() {
        return mathModule;
    }

    public IIOModule.Iface getIoModule() {
        return ioModule;
    }

    public ICacheContextModule.Iface getCacheContextModule() {
        return cacheContextModule;
    }

}
