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
import org.ignis.rpc.IFunction;
import org.ignis.rpc.IRemoteException;
import org.ignis.rpc.driver.IDataId;
import org.ignis.rpc.driver.IDataService;

/**
 *
 * @author César Pomar
 */
public class IDataServiceImpl extends IService implements IDataService.Iface {

    public IDataServiceImpl(IAttributes attributes) {
        super(attributes);
    }

    @Override
    public void keep(IDataId data, byte level) throws IRemoteException, TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            cluster.getJob(data.getJob()).getData(data.getData()).setKeep(level);
        }
    }

    @Override
    public IDataId _map(IDataId data, IFunction _function) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.map(_function);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId streamingMap(IDataId data, IFunction _function, boolean ordered) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.streamingMap(_function, ordered);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId reduceByKey(IDataId data, IFunction _function) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.reduceByKey(_function);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public void saveAsFile(IDataId data, String path, boolean join) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            source.saveAsFile(path, join);
        }
    }

}
