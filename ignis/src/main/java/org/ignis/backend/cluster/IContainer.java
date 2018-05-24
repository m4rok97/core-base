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

import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TZlibTransport;
import org.ignis.backend.allocator.IContainerStub;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.manager.IRegisterManager;
import org.ignis.rpc.manager.IServerManager;

/**
 *
 * @author César Pomar
 */
public class IContainer {

    private final long seqId;
    private final IContainerStub stub;
    private final TTransport transport;
    private final TProtocol protocol;
    private final IProperties properties;
    private final IServerManager.Iface serverManager;
    private final IRegisterManager.Iface registerManager;
    private final List<IExecutor> executors;

    public IContainer(long seqId, IContainerStub stub, IProperties properties) {
        this.seqId = seqId;
        this.stub = stub;
        this.transport = stub.getTransport();
        this.protocol = new TCompactProtocol(new TZlibTransport(transport, 9));//TODO
        this.properties = properties;
        this.serverManager = new IServerManager.Client(protocol);
        this.registerManager = new IRegisterManager.Client(protocol);
        this.executors = new ArrayList<>();
    }

    public IExecutor createExecutor(IProperties properties) {
        IExecutor executor = new IExecutor(executors.size(), null, protocol, properties);
        executors.add(executor);
        return executor;
    }

    public IExecutor createExecutor() {
        return createExecutor(properties);
    }

    public IServerManager.Iface getServerManager() {
        return serverManager;
    }

    public IRegisterManager.Iface getRegisterManager() {
        return registerManager;
    }

}
