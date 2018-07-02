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
import org.ignis.backend.cluster.IData;
import org.ignis.backend.cluster.IJob;
import org.ignis.rpc.IRemoteException;
import org.ignis.rpc.driver.IDataId;
import org.ignis.rpc.driver.IJobId;
import org.ignis.rpc.driver.IJobService;

/**
 *
 * @author César Pomar
 */
public class IJobServiceImpl extends IService implements IJobService.Iface {

    public IJobServiceImpl(IAttributes attributes) {
        super(attributes);
    }

    @Override
    public IJobId newInstance(long cluster, String type) throws IRemoteException, TException {
        IJob job = attributes.getCluster(cluster).createJob(type);
        return new IJobId(cluster, job.getId());
    }

    @Override
    public IJobId newInstance3(long cluster, String type, long properties) throws IRemoteException, TException {
        IJob job = attributes.getCluster(cluster).createJob(type);
        return new IJobId(cluster, job.getId());
    }

    @Override
    public void keep(IJobId job) throws IRemoteException, TException {
        attributes.getCluster(job.getCluster()).getJob(job.getJob()).setKeep(true);
    }

    @Override
    public IDataId importData(IJobId job, IDataId data) throws IRemoteException, TException {
        IData source = attributes.getCluster(data.getCluster()).getJob(data.getJob()).getData(data.getData());
        IData target = attributes.getCluster(job.getCluster()).getJob(job.getJob()).importData(source);
        return new IDataId(job.getCluster(), job.getJob(), target.getId());
    }

    @Override
    public IDataId readFile(IJobId job, String path) throws IRemoteException, TException {
        IData data = attributes.getCluster(job.getCluster()).getJob(job.getJob()).readFile(path);
        return new IDataId(job.getCluster(), job.getJob(), data.getId());
    }

}
