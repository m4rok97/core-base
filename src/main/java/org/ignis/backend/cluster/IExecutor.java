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

import java.util.Map;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TZlibTransport;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.executor.ICacheContextModule;
import org.ignis.rpc.executor.ICommModule;
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

    private class TCompactProtocolExt extends TCompactProtocol {

        public TCompactProtocolExt() {
            super(null);
        }

        public void setTransport(TTransport transport) {
            this.trans_ = transport;
        }

    }

    private final long worker;
    private final int port;
    private final IContainer container;
    private final TCompactProtocolExt protocol;
    private final IExecutorServerModule.Iface executorServerModule;
    private final IGeneralModule.Iface generalModule;
    private final IGeneralActionModule.Iface generalActionModule;
    private final IMathModule.Iface mathModule;
    private final IIOModule.Iface ioModule;
    private final ICacheContextModule.Iface cacheContextModule;
    private final ICommModule.Iface commModule;
    private int pid;
    private int resets;

    public IExecutor(long worker, IContainer container, int port) {
        this.worker = worker;
        this.container = container;
        this.port = port;
        this.resets = -1;
        this.protocol = new TCompactProtocolExt();
        executorServerModule = new IExecutorServerModule.Client(new TMultiplexedProtocol(protocol, "IExecutorServer"));
        generalModule = new IGeneralModule.Client(new TMultiplexedProtocol(protocol, "IGeneral"));
        generalActionModule = new IGeneralActionModule.Client(new TMultiplexedProtocol(protocol, "IGeneralAction"));
        mathModule = new IMathModule.Client(new TMultiplexedProtocol(protocol, "IMath"));
        ioModule = new IIOModule.Client(new TMultiplexedProtocol(protocol, "IIO"));
        cacheContextModule = new ICacheContextModule.Client(new TMultiplexedProtocol(protocol, "ICacheContext"));
        commModule = new ICommModule.Client(new TMultiplexedProtocol(protocol, "IComm"));
    }

    public long getWorker() {
        return worker;
    }

    public long getId() {
        return container.getId();
    }

    public IContainer getContainer() {
        return container;
    }

    public int getPort() {
        return port;
    }

    public IProperties getProperties() {
        return container.getProperties();
    }

    public Map<String, String> getExecutorProperties() {
        Map<String, String> map = getProperties().toMap(true);
        /*Executor dynamic properties*/
        map.put(IKeys.JOB_DIRECTORY, map.get(IKeys.DFS_HOME) + "/" + map.get(IKeys.JOB_NAME));

        return map;
    }

    public boolean isConnected(){
        return protocol.getTransport() != null;
    }
    
    public void connect() throws TException{
        if (isConnected()) {
            disconnect();
        }
        TSocket socket = new TSocket("localhost", port);
        TZlibTransport zlib = new TZlibTransport(socket, container.getProperties().getInteger(IKeys.EXECUTOR_RPC_COMPRESSION));
        protocol.setTransport(zlib);
        zlib.open();
    }
    
    public void disconnect(){
        protocol.getTransport().close();
        protocol.setTransport(null);
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
        resets++;
    }

    public int getResets() {
        return resets;
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

    public ICommModule.Iface getCommModule() {
        return commModule;
    }

}
