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
import org.ignis.backend.cluster.tasks.IMpiConfig;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.rpc.executor.*;
import org.ignis.scheduler.model.IPort;

import java.util.*;

/**
 * @author César Pomar
 */
public final class IExecutor {

    private final long id;
    private final long worker;
    private final int port;
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
    private int pid;
    private int resets;

    public IExecutor(long id, long worker, IContainer container, int port, int cores) {
        this.id = id;
        this.worker = worker;
        this.container = container;
        this.port = port;
        this.cores = cores;
        this.resets = -1;
        this.transport = new ITransportDecorator();
        this.protocol = new TCompactProtocol(transport);
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

    public int getPort() {
        return port;
    }

    public int getCores() {
        return cores;
    }

    public IProperties getProperties() {
        return container.getProperties();
    }

    public Map<String, String> getExecutorProperties() {
        Map<String, String> map = getProperties().toMap(true);
        /*Executor dynamic properties*/
        map.remove(IKeys.DRIVER_PRIVATE_KEY);
        map.put(IKeys.EXECUTOR_CORES, String.valueOf(cores));
        map.put(IKeys.JOB_DIRECTORY, map.get(IKeys.DFS_HOME) + "/" + map.get(IKeys.JOB_NAME));
        map.put(IKeys.JOB_WORKER, String.valueOf(worker));
        map.put(IKeys.EXECUTOR_DIRECTORY, map.get(IKeys.JOB_DIRECTORY) + "/" + container.getCluster() + "/" + worker + "/" + id);
        map.putAll(getUserProperties());

        return map;
    }

    public Map<String, String> getUserProperties() {
        Map<String, String> map = new HashMap<>();
        /*Ports*/
        Set<String> mpiPorts = new HashSet<>();
        for (IPort port : IMpiConfig.getPorts(this)) {
            mpiPorts.add(port.getProtocol() + port.getContainerPort());
        }
        String portPrefix = container.getCluster() < 0 ? IKeys.DRIVER_PORT : IKeys.EXECUTOR_PORT;
        for (IPort port : container.getInfo().getPorts()) {
            if (!mpiPorts.contains(port.getProtocol() + port.getContainerPort())) {
                String key = portPrefix + "." + port.getProtocol() + "." + port.getContainerPort();
                map.put(key, String.valueOf(port.getHostPort()));
            }
        }
        return map;
    }

    public boolean isConnected() {
        return transport.getConcreteTransport() != null && transport.getConcreteTransport().isOpen();
    }

    public void connect() throws TException {
        disconnect();
        TSocket socket = new TSocket("localhost", port);
        TZlibTransport zlib = new TZlibTransport(socket, container.getProperties().getInteger(IKeys.EXECUTOR_RPC_COMPRESSION));
        transport.setConcreteTransport(zlib);
        zlib.open();
    }

    public void disconnect() {
        if (isConnected()) {
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
