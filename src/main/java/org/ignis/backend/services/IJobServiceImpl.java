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
import org.ignis.backend.cluster.IData;
import org.ignis.backend.cluster.IJob;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.IRemoteException;
import org.ignis.rpc.driver.IDataId;
import org.ignis.rpc.driver.IJobId;
import org.ignis.rpc.driver.IJobService;

/**
 *
 * @author César Pomar
 */
public final class IJobServiceImpl extends IService implements IJobService.Iface {
    
    public IJobServiceImpl(IAttributes attributes) {
        super(attributes);
    }
    
    @Override
    public IJobId newInstance(long cluster, String type) throws IRemoteException, TException {
        ICluster clusterObject = attributes.getCluster(cluster);
        synchronized (clusterObject.getLock()) {
            IJob job = clusterObject.createJob(type);
            return new IJobId(cluster, job.getId());
        }
    }
    
    @Override
    public IJobId newInstance3(long cluster, String type, long properties) throws IRemoteException, TException {
        ICluster clusterObject = attributes.getCluster(cluster);
        IProperties propertiesObject = attributes.getProperties(properties);
        IProperties propertiesCopy;
        synchronized (propertiesObject) {
            propertiesCopy = new IProperties(propertiesObject, attributes.defaultProperties);
        }
        synchronized (clusterObject.getLock()) {
            IJob job = clusterObject.createJob(type, propertiesCopy);
            return new IJobId(cluster, job.getId());
        }
    }
    
    @Override
    public void keep(IJobId job) throws IRemoteException, TException {
        ICluster clusterObject = attributes.getCluster(job.getCluster());
        synchronized (clusterObject.getLock()) {
            clusterObject.getJob(job.getJob()).setKeep(true);
        }
    }
    
    @Override
    public IDataId importData(IJobId job, IDataId data) throws IRemoteException, TException {
        ICluster clusterSource = attributes.getCluster(data.getCluster());
        ICluster clusterTarget = attributes.getCluster(job.getCluster());
        ILock lock1 = clusterSource.getLock();
        ILock lock2 = clusterTarget.getLock();
        if (lock1.compareTo(lock2) < 0) {
            ILock tmp = lock1;
            lock1 = lock2;
            lock2 = tmp;
        }
        synchronized (lock1) {
            synchronized (lock2) {
                IData source = clusterSource.getJob(data.getJob()).getData(data.getData());
                IData target = clusterTarget.getJob(job.getJob()).importData(source);
                return new IDataId(job.getCluster(), job.getJob(), target.getId());
            }
        }
    }
    
    @Override
    public IDataId readFile(IJobId job, String path) throws IRemoteException, TException {
        ICluster clusterObject = attributes.getCluster(job.getCluster());
        synchronized (clusterObject.getLock()) {
            IData data = clusterObject.getJob(job.getJob()).readFile(path);
            return new IDataId(job.getCluster(), job.getJob(), data.getId());
        }
    }
    
    @Override
    public void setName(IJobId job, String name) throws IRemoteException, TException {
        ICluster clusterObject = attributes.getCluster(job.getCluster());
        synchronized (clusterObject.getLock()) {
            clusterObject.getJob(job.getJob()).setName(name);
        }
    }
    
}
