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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TZlibTransport;
import org.ignis.backend.unix.ISocket;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.rpc.executor.*;
import org.ignis.scheduler3.ISchedulerParser;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author César Pomar
 */
public final class IExecutor {

    private final long id;
    private final long worker;
    private final int cores;
    private final IContainer container;
    private final ITransportDecorator transport;
    private final TProtocol protocol;
    private final IExecutorServerModule.Iface executorServerModule;
    private final IGeneralModule.Iface generalModule;
    private final IGeneralActionModule.Iface generalActionModule;
    private final IMathModule.Iface mathModule;
    private final IIOModule.Iface ioModule;
    private final ICacheContextModule.Iface cacheContextModule;
    private final ICommModule.Iface commModule;
    private final IProperties context;
    private int pid;
    private int resets;

    public IExecutor(long id, long worker, IContainer container, int cores) {
        this.id = id;
        this.worker = worker;
        this.container = container;
        this.cores = cores;
        this.resets = -1;
        this.transport = new ITransportDecorator();
        this.protocol = new TCompactProtocol(transport);
        this.context = new IProperties();
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
        return id;
    }

    public IContainer getContainer() {
        return container;
    }

    public int getCores() {
        return cores;
    }

    public IProperties getProperties() {
        return container.getProperties();
    }

    private void initContext() {
        context.fromMap(getProperties().toMap(true));
        context.fromMap(new ISchedulerParser(getProperties()).dumpPorts(IKeys.EXECUTOR_PORTS, container.getInfo()));
        if (context.hasProperty(IKeys.CRYPTO_$PRIVATE$)) {
            context.rmProperty(IKeys.CRYPTO_$PRIVATE$);
        }
        context.setProperty(IKeys.EXECUTOR_CORES, String.valueOf(cores));
        context.setProperty(IKeys.JOB_CLUSTER, String.valueOf(container.getCluster()));
        context.setProperty(IKeys.JOB_CONTAINER_ID, String.valueOf(container.getId()));
        context.setProperty(IKeys.JOB_CONTAINER_ID, Path.of(context.getProperty(IKeys.JOB_DIR),
                "tmp", context.getProperty(IKeys.JOB_CONTAINER_ID)).toString());

    }

    public IProperties getContext() {
        return context;
    }

    public boolean isConnected() {
        return transport.getConcreteTransport() != null && transport.getConcreteTransport().isOpen();
    }

    public String getSocket() {
        var name = container.getCluster() + "-" + worker + "-" + id + ".sock";
        return Path.of(getProperties().getProperty(IKeys.JOB_SOCKETS), name).toString();
    }

    public void connect() throws TException {
        connect(getSocket());
    }

    public void connect(String address) throws TException {
        try {
            initContext();
            disconnect();
            var socket = new TSocket(new ISocket(address));
            var zlib = new TZlibTransport(socket, container.getProperties().getInteger(IKeys.TRANSPORT_COMPRESSION));
            transport.setConcreteTransport(zlib);
            zlib.open();
        } catch (IOException ex) {
            throw new TException(ex);
        }
    }

    public void disconnect() {
        if (isConnected()) {
            context.clear();
            try {
                protocol.getTransport().close();
            } catch (Exception ex) {
            }
        }
        transport.setConcreteTransport(null);
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
