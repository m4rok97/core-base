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
import org.ignis.backend.cluster.tasks.IThreadPool;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.scheduler.IScheduler;
import org.ignis.rpc.IRemoteException;
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
    public long newInstance(long properties) throws IRemoteException, TException {
        IProperties propertiesObject = attributes.getProperties(properties);
        IProperties propertiesCopy;
        synchronized (propertiesObject) {
            propertiesCopy = new IProperties(propertiesObject, attributes.defaultProperties);
        }
        long id = attributes.newIdCluster();
        attributes.addCluster(new ICluster(id, propertiesCopy, threadPool, null));//TODO
        return id;
    }

    @Override
    public void keep(long cluster) throws IRemoteException, TException {
        ICluster clusterObject = attributes.getCluster(cluster);
        synchronized (clusterObject.getLock()) {
            clusterObject.setKeep(true);
        }
    }

    @Override
    public int sendFiles(long cluster, String source, String target) throws IRemoteException, TException {
        ICluster clusterObject = attributes.getCluster(cluster);
        synchronized (clusterObject.getLock()) {
            return clusterObject.sendFiles(source, target);
        }
    }

    @Override
    public int sendCompressedFile(long cluster, String source, String target) throws IRemoteException, TException {
        ICluster clusterObject = attributes.getCluster(cluster);
        synchronized (clusterObject.getLock()) {
            return clusterObject.sendCompressedFile(source, target);
        }
    }

    @Override
    public void setName(long cluster, String name) throws IRemoteException, TException {
        ICluster clusterObject = attributes.getCluster(cluster);
        synchronized (clusterObject.getLock()) {
            clusterObject.setName(name);
        }
    }

}
