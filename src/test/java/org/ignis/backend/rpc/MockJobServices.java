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
package org.ignis.backend.rpc;

import java.util.List;
import org.apache.thrift.TException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.IJob;
import org.ignis.rpc.executor.IFilesModule;
import org.ignis.rpc.executor.IKeysModule;
import org.ignis.rpc.executor.IMapperModule;
import org.ignis.rpc.executor.IPostmanModule;
import org.ignis.rpc.executor.IReducerModule;
import org.ignis.rpc.executor.IServerModule;
import org.ignis.rpc.executor.IShuffleModule;
import org.ignis.rpc.executor.ISortModule;
import org.ignis.rpc.executor.IStorageModule;
import org.mockito.Mockito;

/**
 *
 * @author César Pomar
 */
public class MockJobServices {

    private final IJob job;
    private IFilesModule.Iface filesModule;
    private IKeysModule.Iface keysModule;
    private IMapperModule.Iface mapperModule;
    private IPostmanModule.Iface postmanModule;
    private IReducerModule.Iface reducerModule;
    private IServerModule.Iface serverModule;
    private IShuffleModule.Iface shuffleModule;
    private ISortModule.Iface sortModule;
    private IStorageModule.Iface storageModule;

    public MockJobServices(IJob job) {
        this.job = job;
    }

    public IFilesModule.Iface getFilesModule() {
        return filesModule;
    }

    public void setFilesModule(IFilesModule.Iface filesModule) {
        this.filesModule = filesModule;
    }

    public IKeysModule.Iface getKeysModule() {
        return keysModule;
    }

    public void setKeysModule(IKeysModule.Iface keysModule) {
        this.keysModule = keysModule;
    }

    public IMapperModule.Iface getMapperModule() {
        return mapperModule;
    }

    public void setMapperModule(IMapperModule.Iface mapperModule) {
        this.mapperModule = mapperModule;
    }

    public IPostmanModule.Iface getPostmanModule() {
        return postmanModule;
    }

    public void setPostmanModule(IPostmanModule.Iface postmanModule) {
        this.postmanModule = postmanModule;
    }

    public IReducerModule.Iface getReducerModule() {
        return reducerModule;
    }

    public void setReducerModule(IReducerModule.Iface reducerModule) {
        this.reducerModule = reducerModule;
    }

    public IServerModule.Iface getServerModule() {
        return serverModule;
    }

    public void setServerModule(IServerModule.Iface serverModule) {
        this.serverModule = serverModule;
    }

    public IShuffleModule.Iface getShuffleModule() {
        return shuffleModule;
    }

    public void setShuffleModule(IShuffleModule.Iface shuffleModule) {
        this.shuffleModule = shuffleModule;
    }

    public ISortModule.Iface getSortModule() {
        return sortModule;
    }

    public void setSortModule(ISortModule.Iface sortModule) {
        this.sortModule = sortModule;
    }

    public IStorageModule.Iface getStorageModule() {
        return storageModule;
    }

    public void setStorageModule(IStorageModule.Iface storageModule) {
        this.storageModule = storageModule;
    }

    public void mock() {
        for (IExecutor executor: job.getExecutors()) {
            if (filesModule != null) {
                Mockito.when(executor.getFilesModule()).thenReturn(filesModule);
            }
            if (keysModule != null) {
                Mockito.when(executor.getKeysModule()).thenReturn(keysModule);
            }
            if (mapperModule != null) {
                Mockito.when(executor.getMapperModule()).thenReturn(mapperModule);
            }
            if (postmanModule != null) {
                Mockito.when(executor.getPostmanModule()).thenReturn(postmanModule);
            }
            if (reducerModule != null) {
                Mockito.when(executor.getReducerModule()).thenReturn(reducerModule);
            }
            if (serverModule == null) {
                try {
                    serverModule = Mockito.mock(IServerModule.Iface.class);
                    Mockito.doAnswer(a -> null).when(serverModule).test();
                    Mockito.doAnswer(a -> null).when(serverModule).stop();
                    Mockito.doAnswer(a -> null).when(serverModule).updateProperties(Mockito.any());
                } catch (TException ex) {
                }
            }
            Mockito.when(executor.getServerModule()).thenReturn(serverModule);

            if (shuffleModule != null) {
                Mockito.when(executor.getShuffleModule()).thenReturn(shuffleModule);
            }
            if (sortModule != null) {
                Mockito.when(executor.getSortModule()).thenReturn(sortModule);
            }
            if (storageModule != null) {
                Mockito.when(executor.getStorageModule()).thenReturn(storageModule);
            }
        }
    }

}
