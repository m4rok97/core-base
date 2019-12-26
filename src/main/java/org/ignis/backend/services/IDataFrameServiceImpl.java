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
import org.ignis.backend.cluster.helpers.dataframe.IDataGeneralActionHelper;
import org.ignis.backend.cluster.helpers.dataframe.IDataGeneralHelper;
import org.ignis.backend.cluster.helpers.dataframe.IDataIOHelper;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.exception.IDriverExceptionImpl;
import org.ignis.rpc.IDriverException;
import org.ignis.rpc.ISource;
import org.ignis.rpc.driver.IDataFrameId;
import org.ignis.rpc.driver.IDataFrameService;

/**
 *
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
            synchronized (worker.getLock()) {
                worker.getDataFrame(id.getDataFrame()).setName(name);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void saveAsPartitionObjectFile(IDataFrameId id, String path, byte compression) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = new IDataIOHelper(data, worker.getProperties()).saveAsPartitionObjectFile(path, compression);
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
                result = new IDataIOHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).saveAsTextFile(path);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void saveAsJsonFile(IDataFrameId id, String path) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = new IDataIOHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).saveAsJsonFile(path);
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
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).map(src);
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
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).filter(src);
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
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).flatmap(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId mapPartitions(IDataFrameId id, ISource src, boolean preservesPartitioning) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).mapPartitions(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId mapPartitionsWithIndex(IDataFrameId id, ISource src, boolean preservesPartitioning) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).mapPartitionsWithIndex(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId applyPartition(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).applyPartition(src);
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
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).groupBy(src);
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
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).groupBy(src, numPartitions);
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
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).sort(ascending);
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
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).sort(ascending, numPartitions);
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
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).sortBy(src, ascending);
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
                IDataFrame result = new IDataGeneralHelper(worker.getDataFrame(id.getDataFrame()), worker.getProperties()).sortBy(src, ascending, numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long reduce(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).reduce(src);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long treeReduce(IDataFrameId id, ISource src, long depth) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).treeReduce(src);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long collect(IDataFrameId id) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).collect(attributes.driver);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long aggregate(IDataFrameId id, ISource seqOp, ISource combOp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).aggregate(seqOp, combOp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long treeAggregate(IDataFrameId id, ISource seqOp, ISource combOp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).treeAggregate(seqOp, combOp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long treeAggregate3(IDataFrameId id, ISource seqOp, ISource combOp, long depth) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).treeAggregate(seqOp, combOp, depth);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long fold(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).fold(src);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long take(IDataFrameId id, long num) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).take(num);
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
    public long top(IDataFrameId id, long num) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).top(num);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long top2(IDataFrameId id, long num, ISource cmp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).top(num, cmp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

}
