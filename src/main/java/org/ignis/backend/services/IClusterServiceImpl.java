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

import java.util.List;
import org.apache.thrift.TException;
import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.cluster.helpers.cluster.IClusterExecuteHelper;
import org.ignis.backend.cluster.helpers.cluster.IClusterFileHelper;
import org.ignis.backend.cluster.tasks.IThreadPool;
import org.ignis.backend.exception.IDriverExceptionImpl;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.scheduler.IScheduler;
import org.ignis.rpc.IDriverException;
import org.ignis.rpc.driver.IClusterService;

/**
 *
 * @author César Pomar
 */
public final class IClusterServiceImpl extends IService implements IClusterService.Iface {

    private final IThreadPool threadPool;
    private final IScheduler scheduler;

    public IClusterServiceImpl(IAttributes attributes, IScheduler scheduler) throws IgnisException {
        super(attributes);
        int minWorkers = attributes.defaultProperties.getInteger(IKeys.DRIVER_TASK_MIN_WORKERS);
        int maxFailures = attributes.defaultProperties.getInteger(IKeys.DRIVER_TASK_MAX_FAILURES);
        this.threadPool = new IThreadPool(minWorkers, maxFailures);
        this.scheduler = scheduler;
    }

    @Override
    public long newInstance(long properties) throws IDriverException, TException {
        return newInstance2("", properties);
    }

    @Override
    public long newInstance2(String name, long properties) throws IDriverException, TException {
        try {
            IProperties propertiesObject = attributes.getProperties(properties);
            IProperties clusterProperties;
            synchronized (propertiesObject) {
                clusterProperties = new IProperties(propertiesObject, attributes.defaultProperties);
            }
            long id = attributes.newCluster();
            attributes.setCluster(new ICluster(name, id, clusterProperties, threadPool, scheduler, attributes.ssh));
            return id;
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void setName(long cluster, String name) throws IDriverException, TException {
        try {
            ICluster clusterObject = attributes.getCluster(cluster);
            synchronized (clusterObject.getLock()) {
                clusterObject.setName(name);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void execute(long id, List<String> cmd) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id);
            synchronized (cluster.getLock()) {
                new IClusterExecuteHelper(cluster, cluster.getProperties()).execute(cmd);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void executeScript(long id, String script) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id);
            synchronized (cluster.getLock()) {
                new IClusterExecuteHelper(cluster, cluster.getProperties()).executeScript(script);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void sendFile(long id, String source, String target) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id);
            synchronized (cluster.getLock()) {
                new IClusterFileHelper(cluster, cluster.getProperties()).sendFile(source, target);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void sendCompressedFile(long id, String source, String target) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id);
            synchronized (cluster.getLock()) {
                new IClusterFileHelper(cluster, cluster.getProperties()).sendCompressedFile(source, target);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    public void destroyClusters() {
        for (ICluster cluster : attributes.getClusters()) {
            try {
                cluster.destroy(scheduler);
            } catch (IgnisException ex) {
            }
        }
    }

}
