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
import org.ignis.backend.cluster.IDataFrame;
import org.ignis.backend.cluster.IWorker;
import org.ignis.backend.cluster.helpers.dataframe.*;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.exception.IDriverExceptionImpl;
import org.ignis.rpc.ISource;
import org.ignis.rpc.driver.IDataFrameId;
import org.ignis.rpc.driver.IDataFrameService;
import org.ignis.rpc.driver.IDriverException;

/**
 * @author César Pomar
 */
public final class IDataFrameServiceImpl extends IService implements IDataFrameService.Iface {

    public IDataFrameServiceImpl(IAttributes attributes) {
        super(attributes);
    }

    @Override
    public void setName(IDataFrameId id, String name) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                data.setName(name);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void persist(IDataFrameId id, byte level) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                new IDataCacheHelper(data, worker.getProperties()).persist(level);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void cache(IDataFrameId id) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                new IDataCacheHelper(data, worker.getProperties()).cache();
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void unpersist(IDataFrameId id) throws IDriverException, TException {
        uncache(id);
    }

    @Override
    public void uncache(IDataFrameId id) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                new IDataCacheHelper(data, worker.getProperties()).uncache();
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId repartition(IDataFrameId id, long numPartitions, boolean preserveOrdering, boolean global_) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).repartition(numPartitions, preserveOrdering, global_);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId partitionByRandom(IDataFrameId id, long numPartitions, int seed) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).partitionByRandom(numPartitions, seed);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId partitionByHash(IDataFrameId id, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).partitionByHash(numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId partitionBy(IDataFrameId id, ISource src, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).partitionBy(src, numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long partitions(IDataFrameId id) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataIOHelper(data, worker.getProperties()).partitions();
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void saveAsObjectFile(IDataFrameId id, String path, byte compression) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = new IDataIOHelper(data, worker.getProperties()).saveAsObjectFile(path, compression);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void saveAsTextFile(IDataFrameId id, String path) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = new IDataIOHelper(data, worker.getProperties()).saveAsTextFile(path);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void saveAsJsonFile(IDataFrameId id, String path, boolean pretty) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = new IDataIOHelper(data, worker.getProperties()).saveAsJsonFile(path, pretty);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId map_(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).map(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId filter(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).filter(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId flatmap(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).flatmap(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId keyBy(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).keyBy(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId mapWithIndex(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).mapWithIndex(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId mapPartitions(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).mapPartitions(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId mapPartitionsWithIndex(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).mapPartitionsWithIndex(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId mapExecutor(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).mapExecutor(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId mapExecutorTo(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).mapExecutorTo(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId groupBy(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).groupBy(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId groupBy2(IDataFrameId id, ISource src, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).groupBy(src, numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId sort(IDataFrameId id, boolean ascending) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).sort(ascending);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId sort2(IDataFrameId id, boolean ascending, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).sort(ascending, numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId sortBy(IDataFrameId id, ISource src, boolean ascending) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).sortBy(src, ascending);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId sortBy3(IDataFrameId id, ISource src, boolean ascending, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).sortBy(src, ascending, numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId union_(IDataFrameId id, IDataFrameId other, boolean preserveOrder) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            ICluster clusterOther = attributes.getCluster(other.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IWorker workerOther = clusterOther.getWorker(other.getWorker());

            ILock lock1 = worker.getLock();
            ILock lock2 = workerOther.getLock();
            if (lock1.compareTo(lock2) < 0) {
                ILock tmp = lock1;
                lock1 = lock2;
                lock2 = tmp;
            }
            synchronized (lock1) {
                synchronized (lock2) {
                    IDataFrame data = worker.getDataFrame(id.getDataFrame());
                    IDataFrame dataOther = workerOther.getDataFrame(other.getDataFrame());
                    IDataFrame target = new IDataGeneralHelper(data, data.getProperties()).union(dataOther, preserveOrder);
                    return new IDataFrameId(cluster.getId(), worker.getId(), target.getId());
                }
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId union4(IDataFrameId id, IDataFrameId other, boolean preserveOrder, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            ICluster clusterOther = attributes.getCluster(other.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IWorker workerOther = clusterOther.getWorker(other.getWorker());

            ILock lock1 = worker.getLock();
            ILock lock2 = workerOther.getLock();
            if (lock1.compareTo(lock2) < 0) {
                ILock tmp = lock1;
                lock1 = lock2;
                lock2 = tmp;
            }
            synchronized (lock1) {
                synchronized (lock2) {
                    IDataFrame data = worker.getDataFrame(id.getDataFrame());
                    IDataFrame dataOther = workerOther.getDataFrame(other.getDataFrame());
                    IDataFrame target = new IDataGeneralHelper(data, data.getProperties()).union(dataOther, preserveOrder, src);
                    return new IDataFrameId(cluster.getId(), worker.getId(), target.getId());
                }
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId join(IDataFrameId id, IDataFrameId other) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            ICluster clusterOther = attributes.getCluster(other.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IWorker workerOther = clusterOther.getWorker(other.getWorker());

            ILock lock1 = worker.getLock();
            ILock lock2 = workerOther.getLock();
            if (lock1.compareTo(lock2) < 0) {
                ILock tmp = lock1;
                lock1 = lock2;
                lock2 = tmp;
            }
            synchronized (lock1) {
                synchronized (lock2) {
                    IDataFrame data = worker.getDataFrame(id.getDataFrame());
                    IDataFrame dataOther = workerOther.getDataFrame(other.getDataFrame());
                    IDataFrame target = new IDataGeneralHelper(data, data.getProperties()).join(dataOther);
                    return new IDataFrameId(cluster.getId(), worker.getId(), target.getId());
                }
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId join3a(IDataFrameId id, IDataFrameId other, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            ICluster clusterOther = attributes.getCluster(other.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IWorker workerOther = clusterOther.getWorker(other.getWorker());

            ILock lock1 = worker.getLock();
            ILock lock2 = workerOther.getLock();
            if (lock1.compareTo(lock2) < 0) {
                ILock tmp = lock1;
                lock1 = lock2;
                lock2 = tmp;
            }
            synchronized (lock1) {
                synchronized (lock2) {
                    IDataFrame data = worker.getDataFrame(id.getDataFrame());
                    IDataFrame dataOther = workerOther.getDataFrame(other.getDataFrame());
                    IDataFrame target = new IDataGeneralHelper(data, data.getProperties()).join(dataOther, numPartitions);
                    return new IDataFrameId(cluster.getId(), worker.getId(), target.getId());
                }
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId join3b(IDataFrameId id, IDataFrameId other, ISource src) throws TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            ICluster clusterOther = attributes.getCluster(other.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IWorker workerOther = clusterOther.getWorker(other.getWorker());

            ILock lock1 = worker.getLock();
            ILock lock2 = workerOther.getLock();
            if (lock1.compareTo(lock2) < 0) {
                ILock tmp = lock1;
                lock1 = lock2;
                lock2 = tmp;
            }
            synchronized (lock1) {
                synchronized (lock2) {
                    IDataFrame data = worker.getDataFrame(id.getDataFrame());
                    IDataFrame dataOther = workerOther.getDataFrame(other.getDataFrame());
                    IDataFrame target = new IDataGeneralHelper(data, data.getProperties()).join(dataOther, src);
                    return new IDataFrameId(cluster.getId(), worker.getId(), target.getId());
                }
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId join4(IDataFrameId id, IDataFrameId other, long numPartitions, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            ICluster clusterOther = attributes.getCluster(other.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IWorker workerOther = clusterOther.getWorker(other.getWorker());

            ILock lock1 = worker.getLock();
            ILock lock2 = workerOther.getLock();
            if (lock1.compareTo(lock2) < 0) {
                ILock tmp = lock1;
                lock1 = lock2;
                lock2 = tmp;
            }
            synchronized (lock1) {
                synchronized (lock2) {
                    IDataFrame data = worker.getDataFrame(id.getDataFrame());
                    IDataFrame dataOther = workerOther.getDataFrame(other.getDataFrame());
                    IDataFrame target = new IDataGeneralHelper(data, data.getProperties()).join(dataOther, numPartitions, src);
                    return new IDataFrameId(cluster.getId(), worker.getId(), target.getId());
                }
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId distinct(IDataFrameId id) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).distinct();
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId distinct2a(IDataFrameId id, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).distinct(numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId distinct2b(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).distinct(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId distinct3(IDataFrameId id, long numPartitions, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).distinct(numPartitions, src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long reduce(IDataFrameId id, ISource src, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).reduce(attributes.driver, src, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long treeReduce(IDataFrameId id, ISource src, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).treeReduce(attributes.driver, src, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long collect(IDataFrameId id, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).collect(attributes.driver, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long aggregate(IDataFrameId id, ISource zero, ISource seqOp, ISource combOp, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).aggregate(attributes.driver, zero, seqOp, combOp, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long treeAggregate(IDataFrameId id, ISource zero, ISource seqOp, ISource combOp, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).treeAggregate(attributes.driver, zero, seqOp, combOp, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long fold(IDataFrameId id, ISource zero, ISource src, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).fold(attributes.driver, zero, src, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long treeFold(IDataFrameId id, ISource zero, ISource src, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).treeFold(attributes.driver, zero, src, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long take(IDataFrameId id, long num, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).take(attributes.driver, num, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void foreach_(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).foreach(src);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void foreachPartition(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).foreachPartition(src);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void foreachExecutor(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).foreachExecutor(src);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long top(IDataFrameId id, long num, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).top(attributes.driver, num, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long top4(IDataFrameId id, long num, ISource cmp, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).top(attributes.driver, num, cmp, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long takeOrdered(IDataFrameId id, long num, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).takeOrdered(attributes.driver, num, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long takeOrdered4(IDataFrameId id, long num, ISource cmp, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).takeOrdered(attributes.driver, num, cmp, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId sample(IDataFrameId id, boolean withReplacement, double fraction, int seed) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataMathHelper(data, worker.getProperties()).sample(withReplacement, fraction, seed);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long takeSample(IDataFrameId id, boolean withReplacement, long num, int seed, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataMathHelper(data, worker.getProperties()).takeSample(attributes.driver, withReplacement, num, seed, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long count(IDataFrameId id) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataMathHelper(data, worker.getProperties()).count();
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long max(IDataFrameId id, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataMathHelper(data, worker.getProperties()).max(attributes.driver, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long max3(IDataFrameId id, ISource cmp, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataMathHelper(data, worker.getProperties()).max(attributes.driver, cmp, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long min(IDataFrameId id, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataMathHelper(data, worker.getProperties()).min(attributes.driver, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long min3(IDataFrameId id, ISource cmp, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataMathHelper(data, worker.getProperties()).min(attributes.driver, cmp, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId flatMapValues(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).flatMapValues(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId mapValues(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).mapValues(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId groupByKey(IDataFrameId id) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).groupByKey();
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId groupByKey2a(IDataFrameId id, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).groupByKey(numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId groupByKey2b(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).groupByKey(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId groupByKey3(IDataFrameId id, long numPartitions, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).groupByKey(numPartitions, src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId reduceByKey(IDataFrameId id, ISource src, boolean localReduce) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).reduceByKey(src, localReduce);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId reduceByKey4(IDataFrameId id, ISource src, long numPartitions, boolean localReduce) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).reduceByKey(src, numPartitions, localReduce);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId aggregateByKey(IDataFrameId id, ISource zero, ISource seqOp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).aggregateByKey(zero, seqOp);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId aggregateByKey4a(IDataFrameId id, ISource zero, ISource seqOp, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).aggregateByKey(zero, seqOp, numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId aggregateByKey4b(IDataFrameId id, ISource zero, ISource seqOp, ISource combOp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).aggregateByKey(zero, seqOp, combOp);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId aggregateByKey5(IDataFrameId id, ISource zero, ISource seqOp, ISource combOp, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).aggregateByKey(zero, seqOp, combOp, numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId foldByKey(IDataFrameId id, ISource zero, ISource src, boolean localFold) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).foldByKey(zero, src, localFold);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId foldByKey5(IDataFrameId id, ISource zero, ISource src, long numPartitions, boolean localFold) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).foldByKey(zero, src, numPartitions, localFold);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId sortByKey(IDataFrameId id, boolean ascending) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).sortByKey(ascending);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId sortByKey3a(IDataFrameId id, boolean ascending, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).sortByKey(ascending, numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId sortByKey3b(IDataFrameId id, ISource src, boolean ascending) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).sortByKey(src, ascending);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId sortByKey4(IDataFrameId id, ISource src, boolean ascending, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).sortByKey(src, ascending, numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long keys(IDataFrameId id, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).keys(attributes.driver, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long values(IDataFrameId id, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).values(attributes.driver, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId sampleByKey(IDataFrameId id, boolean withReplacement, ISource fractions, int seed) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataMathHelper(data, worker.getProperties()).sampleByKey(withReplacement, fractions, seed);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long countByKey(IDataFrameId id, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataMathHelper(data, worker.getProperties()).countByKey(attributes.driver, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long countByValue(IDataFrameId id, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataMathHelper(data, worker.getProperties()).countByValue(attributes.driver, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }
}
