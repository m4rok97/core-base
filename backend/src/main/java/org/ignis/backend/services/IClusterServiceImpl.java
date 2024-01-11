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
package org.ignis.backend.services;

import org.apache.thrift.TException;
import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.cluster.helpers.cluster.IClusterExecuteHelper;
import org.ignis.backend.cluster.helpers.cluster.IClusterFileHelper;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.exception.IDriverExceptionImpl;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.rpc.driver.IClusterService;
import org.ignis.rpc.driver.IDriverException;
import org.ignis.scheduler3.IScheduler;

import java.util.List;

/**
 * @author César Pomar
 */
public final class IClusterServiceImpl extends IService implements IClusterService.Iface {

    private final IScheduler scheduler;

    public IClusterServiceImpl(IServiceStorage ss, IScheduler scheduler) throws IgnisException {
        super(ss);
        this.scheduler = scheduler;
    }

    @Override
    public void start(long id) throws TException {
        try {
            ICluster clusterObject = ss.getCluster(id);
            ILazy<Void> result;
            synchronized (clusterObject.getLock()) {
                result = clusterObject.start();
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void destroy(long id) throws TException {
        try {
            ICluster clusterObject = ss.getCluster(id);
            ILazy<Void> result;
            synchronized (clusterObject.getLock()) {
                result = clusterObject.destroy(scheduler);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long newInstance0() throws IDriverException, TException {
        return newInstance1a(null);
    }

    @Override
    public long newInstance1a(String name) throws IDriverException, TException {
        try {
            IProperties propertiesObject = ss.props().copy();
            long id = ss.newCluster();
            if (name == null) {
                name = propertiesObject.getProperty(IKeys.EXECUTOR_NAME);
            }
            ss.setCluster(new ICluster(name, id, propertiesObject, ss.pool(), scheduler));
            return id;
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long newInstance1b(long properties) throws IDriverException, TException {
        return newInstance2(null, properties);
    }

    @Override
    public long newInstance2(String name, long properties) throws IDriverException, TException {
        try {
            IProperties propertiesObject = ss.getProperties(properties);
            IProperties clusterProperties;
            synchronized (propertiesObject) {
                clusterProperties = propertiesObject.copy();
            }
            long id = ss.newCluster();
            if (name == null) {
                name = clusterProperties.getProperty(IKeys.EXECUTOR_NAME);
            }
            ss.setCluster(new ICluster(name, id, clusterProperties, ss.pool(), scheduler));
            return id;
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void setName(long cluster, String name) throws IDriverException, TException {
        try {
            throw new UnsupportedOperationException("Cluster renaming is no longer supported");
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void execute(long id, List<String> cmd) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id);
            ILazy<Void> result;
            synchronized (cluster.getLock()) {
                result = new IClusterExecuteHelper(cluster, cluster.getProperties()).execute(cmd);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void executeScript(long id, String script) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id);
            ILazy<Void> result;
            synchronized (cluster.getLock()) {
                result = new IClusterExecuteHelper(cluster, cluster.getProperties()).executeScript(script);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void sendFile(long id, String source, String target) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id);
            ILazy<Void> result;
            synchronized (cluster.getLock()) {
                result = new IClusterFileHelper(cluster, cluster.getProperties()).sendFile(source, target);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void sendCompressedFile(long id, String source, String target) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id);
            ILazy<Void> result;
            synchronized (cluster.getLock()) {
                result = new IClusterFileHelper(cluster, cluster.getProperties()).sendCompressedFile(source, target);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    public void destroyClusters() {
        for (ICluster cluster : ss.getClusters()) {
            try {
                cluster.destroy(scheduler).execute();
            } catch (IgnisException ex) {
            }
        }
    }

}
