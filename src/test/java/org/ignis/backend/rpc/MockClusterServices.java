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
import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.manager.IFileManager;
import org.ignis.rpc.manager.IRegisterManager;
import org.ignis.rpc.manager.IServerManager;
import org.mockito.Mockito;

/**
 *
 * @author César Pomar
 */
public class MockClusterServices {

    private final ICluster cluster;
    private IServerManager.Iface serverManager;
    private IRegisterManager.Iface registerManager;
    private IFileManager.Iface fileManager;

    public MockClusterServices(ICluster cluster) {
        this.cluster = cluster;
    }

    public IServerManager.Iface getServerManager() {
        return serverManager;
    }

    public void setServerManager(IServerManager.Iface serverManager) {
        this.serverManager = serverManager;
    }

    public IRegisterManager.Iface getRegisterManager() {
        return registerManager;
    }

    public void setRegisterManager(IRegisterManager.Iface registerManager) {
        this.registerManager = registerManager;
    }

    public IFileManager.Iface getFileManager() {
        return fileManager;
    }

    public void setFileManager(IFileManager.Iface fileManager) {
        this.fileManager = fileManager;
    }

    public void mock() {
        List<IContainer> containers = cluster.getContainers();
        cluster.putScheduler(new ITaskGroup.Builder(cluster.getLock()).build());//Avoid Create Task
        
        for (int i = 0; i < containers.size(); i++) {
            IContainer container = Mockito.spy(containers.get(i));
            try {
                Mockito.doAnswer(a -> null).when(container).connect();
                Mockito.doAnswer(a -> Mockito.spy(a.callRealMethod())).when(container).createExecutor(Mockito.anyLong(), Mockito.any());
            } catch (IgnisException ex) {
            }
            if (serverManager != null) {
                Mockito.when(container.getServerManager()).thenReturn(serverManager);
            } 
            if (registerManager != null) {
                Mockito.when(container.getRegisterManager()).thenReturn(registerManager);
            }
            if (fileManager != null) {
                Mockito.when(container.getFileManager()).thenReturn(fileManager);
            }
            containers.set(i, container);
        }
    }
}
