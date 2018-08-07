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
import java.util.Collections;
import java.util.List;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TZlibTransport;
import org.ignis.backend.allocator.IContainerStub;
import org.ignis.backend.allocator.IExecutorStub;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.cluster.tasks.Task;
import org.ignis.backend.cluster.tasks.container.IContainerCreateTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IPropertiesKeys;
import org.ignis.backend.properties.IPropertiesParser;
import org.ignis.rpc.manager.IFileManager;
import org.ignis.rpc.manager.IRegisterManager;
import org.ignis.rpc.manager.IServerManager;

/**
 *
 * @author César Pomar
 */
public class IContainer {

    private final IContainerStub stub;
    private final IProperties properties;
    private final TTransport transport;
    private final TProtocol protocol;
    private final IServerManager.Iface serverManager;
    private final IRegisterManager.Iface registerManager;
    private final IFileManager.Iface fileManager;
    private final List<Task> tasks;

    public IContainer(IContainerStub stub, IProperties properties, ILock lock) throws IgnisException {
        this.stub = stub;
        this.properties = properties;
        this.transport = stub.getTransport();
        this.protocol = new TCompactProtocol(new TZlibTransport(transport,
                IPropertiesParser.getInteger(properties, IPropertiesKeys.MANAGER_RPC_COMPRESSION)));
        this.serverManager = new IServerManager.Client(new TMultiplexedProtocol(protocol, "server"));
        this.registerManager = new IRegisterManager.Client(new TMultiplexedProtocol(protocol, "register"));
        this.fileManager = new IFileManager.Client(new TMultiplexedProtocol(protocol, "file"));
        this.tasks = new ArrayList<>();
        this.tasks.add(new IContainerCreateTask(this, lock));
    }

    public IProperties getProperties() {
        return properties;
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

    public void pushTask(Task task) {
        if (task != null) {
            this.tasks.add(task);
        }
    }

    public List<Task> getTasks() {
        return Collections.unmodifiableList(tasks);
    }

    public Task getTask() {
        return tasks.get(tasks.size() - 1);
    }

    public IContainerStub getStub() {
        return stub;
    }

    public String getHost() {
        return stub.getHost();
    }

    public int getPortAlias(int port) {
        return stub.getPortAlias(port);
    }

}
