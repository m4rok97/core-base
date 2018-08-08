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

import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.ignis.backend.allocator.IExecutorStub;
import org.ignis.rpc.executor.IFilesModule;
import org.ignis.rpc.executor.IKeysModule;
import org.ignis.rpc.executor.IMapperModule;
import org.ignis.rpc.executor.IPostmanModule;
import org.ignis.rpc.executor.IReducerModule;
import org.ignis.rpc.executor.IServerModule;
import org.ignis.rpc.executor.IShuffleModule;
import org.ignis.rpc.executor.ISortModule;
import org.ignis.rpc.executor.IStorageModule;

/**
 *
 * @author César Pomar
 */
public class IExecutor {

    private final long job;
    private final IContainer container;
    private final IExecutorStub stub;
    private final TProtocol protocol;
    private final IFilesModule.Iface filesModule;
    private final IKeysModule.Iface keysModule;
    private final IMapperModule.Iface mapperModule;
    private final IPostmanModule.Iface postmanModule;
    private final IReducerModule.Iface reducerModule;
    private final IServerModule.Iface serverModule;
    private final IShuffleModule.Iface shuffleModule;
    private final ISortModule.Iface sortModule;
    private final IStorageModule.Iface storageModule;

    public IExecutor(long job, IContainer container, IExecutorStub stub, TProtocol protocol) {
        this.job = job;
        this.container = container;
        this.stub = stub;
        this.protocol = protocol;
        this.filesModule = new IFilesModule.Client(new TMultiplexedProtocol(protocol, "files" + job));
        this.keysModule = new IKeysModule.Client(new TMultiplexedProtocol(protocol, "keys" + job));
        this.mapperModule = new IMapperModule.Client(new TMultiplexedProtocol(protocol, "mapper" + job));
        this.postmanModule = new IPostmanModule.Client(new TMultiplexedProtocol(protocol, "postman" + job));
        this.reducerModule = new IReducerModule.Client(new TMultiplexedProtocol(protocol, "reducer" + job));
        this.serverModule = new IServerModule.Client(new TMultiplexedProtocol(protocol, "server" + job));
        this.shuffleModule = new IShuffleModule.Client(new TMultiplexedProtocol(protocol, "shuffle" + job));
        this.sortModule = new ISortModule.Client(new TMultiplexedProtocol(protocol, "sort" + job));
        this.storageModule = new IStorageModule.Client(new TMultiplexedProtocol(protocol, "storage" + job));
    }

    public long getJob() {
        return job;
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

    public IKeysModule.Iface getKeysModule() {
        return keysModule;
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

    public IShuffleModule.Iface getShuffleModule() {
        return shuffleModule;
    }

    public IStorageModule.Iface getStorageModule() {
        return storageModule;
    }

}
