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

import org.apache.thrift.protocol.TProtocol;
import org.ignis.backend.allocator.IExecutorStub;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.executor.IFilesModule;
import org.ignis.rpc.executor.IMapperModule;
import org.ignis.rpc.executor.IPostmanModule;
import org.ignis.rpc.executor.IReducerModule;
import org.ignis.rpc.executor.IServerModule;
import org.ignis.rpc.executor.ISortModule;
import org.ignis.rpc.executor.IStorageModule;

/**
 *
 * @author César Pomar
 */
public class IExecutor {

    private final IContainer container;
    private final IExecutorStub stub;
    private final TProtocol protocol;
    private final IProperties properties;
    private final IFilesModule.Iface filesModule;
    private final IMapperModule.Iface mapperModule;
    private final IPostmanModule.Iface postmanModule;
    private final IReducerModule.Iface reducerModule;
    private final IServerModule.Iface serverModule;
    private final ISortModule.Iface sortModule;
    private final IStorageModule.Iface storageModule;

    public IExecutor(IContainer container, IExecutorStub stub, TProtocol protocol, IProperties properties) {
        this.container = container;
        this.stub = stub;
        this.protocol = protocol;
        this.properties = properties;
        this.filesModule = new IFilesModule.Client(protocol);
        this.mapperModule = new IMapperModule.Client(protocol);
        this.postmanModule = new IPostmanModule.Client(protocol);
        this.reducerModule = new IReducerModule.Client(protocol);
        this.serverModule = new IServerModule.Client(protocol);
        this.sortModule = new ISortModule.Client(protocol);
        this.storageModule = new IStorageModule.Client(protocol);
    }

    public IContainer getContainer() {
        return container;
    }

    public TProtocol getProtocol() {
        return protocol;
    }

    public IExecutorStub getStub() {
        return stub;
    }

    public IFilesModule.Iface getFilesModule() {
        return filesModule;
    }

    public IMapperModule.Iface getMapperModule() {
        return mapperModule;
    }

    public IPostmanModule.Iface getPostmanModule() {
        return postmanModule;
    }

    public IReducerModule.Iface getReducerModule() {
        return reducerModule;
    }

    public IServerModule.Iface getServerModule() {
        return serverModule;
    }

    public ISortModule.Iface getSortModule() {
        return sortModule;
    }

    public IStorageModule.Iface getStorageModule() {
        return storageModule;
    }

}
