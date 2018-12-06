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

import java.nio.ByteBuffer;
import org.apache.thrift.TException;
import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.cluster.IData;
import org.ignis.rpc.IRemoteException;
import org.ignis.rpc.ISource;
import org.ignis.rpc.driver.IDataId;
import org.ignis.rpc.driver.IDataService;
import org.ignis.backend.cluster.tasks.ILazy;

/**
 *
 * @author César Pomar
 */
public final class IDataServiceImpl extends IService implements IDataService.Iface {

    public IDataServiceImpl(IAttributes attributes) {
        super(attributes);
    }

    @Override
    public void setName(IDataId data, String name) throws IRemoteException, TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            cluster.getJob(data.getJob()).getData(data.getData()).setName(name);
        }
    }

    @Override
    public IDataId _map(IDataId data, ISource _function) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.map(_function);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId flatmap(IDataId data, ISource _function) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.flatmap(_function);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId filter(IDataId data, ISource _function) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.filter(_function);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId keyBy(IDataId data, ISource _function) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.keyBy(_function);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId streamingMap(IDataId data, ISource _function, boolean ordered) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.streamingMap(_function, ordered);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId streamingFlatmap(IDataId data, ISource _function, boolean ordered) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.streamingFlatmap(_function, ordered);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId streamingFilter(IDataId data, ISource _function, boolean ordered) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.streamingFilter(_function, ordered);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId reduceByKey(IDataId data, ISource _function) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.reduceByKey(_function);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId streamingKeyBy(IDataId data, ISource _function, boolean ordered) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.streamingKeyBy(_function, ordered);
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId values(IDataId data) throws IRemoteException, TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.values();
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId shuffle(IDataId data) throws IRemoteException, TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            IData target = source.shuffle();
            return new IDataId(data.getCluster(), data.getJob(), target.getId());
        }
    }

    @Override
    public IDataId parallelize() throws IRemoteException, TException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ByteBuffer take(IDataId data, long n, boolean light) throws IRemoteException, TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        ILazy<ByteBuffer> result;
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            result = source.take(n, light);
        }
        return result.execute();
    }

    @Override
    public ByteBuffer takeSample(IDataId data, long n, boolean withRemplacement, int seed, boolean light) throws IRemoteException, TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        ILazy<ByteBuffer> result;
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            result = source.takeSample(n, withRemplacement, seed, light);
        }
        return result.execute();
    }

    @Override
    public ByteBuffer collect(IDataId data, boolean light) throws IRemoteException, TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        ILazy<ByteBuffer> result;
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            result = source.collect(light);
        }
        return result.execute();
    }

    @Override
    public void saveAsTextFile(IDataId data, String path, boolean join) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        ILazy<Void> result;
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            result = source.saveAsTextFile(path, join);
        }
        result.execute();
    }

    @Override
    public void saveAsJsonFile(IDataId data, String path, boolean join) throws TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        ILazy<Void> result;
        synchronized (cluster.getLock()) {
            IData source = cluster.getJob(data.getJob()).getData(data.getData());
            result = source.saveAsJsonFile(path, join);
        }
        result.execute();
    }

    @Override
    public void cache(IDataId data) throws IRemoteException, TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            cluster.getJob(data.getJob()).getData(data.getData()).cache();
        }
    }

    @Override
    public void uncache(IDataId data) throws IRemoteException, TException {
        ICluster cluster = attributes.getCluster(data.getCluster());
        synchronized (cluster.getLock()) {
            cluster.getJob(data.getJob()).getData(data.getData()).uncache();
        }
    }

}
