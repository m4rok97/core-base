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
import org.ignis.backend.cluster.helpers.dataframe.IDataCacheHelper;
import org.ignis.backend.cluster.helpers.dataframe.IDataGeneralActionHelper;
import org.ignis.backend.cluster.helpers.dataframe.IDataGeneralHelper;
import org.ignis.backend.cluster.helpers.dataframe.IDataIOHelper;
import org.ignis.backend.cluster.helpers.dataframe.IDataMathHelper;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.exception.IDriverExceptionImpl;
import org.ignis.rpc.driver.IDriverException;
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
    public IDataFrameId repartition(IDataFrameId id, long numPartitions) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataIOHelper(data, worker.getProperties()).repartition(numPartitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), result.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId coalesce(IDataFrameId id, long numPartitions, boolean shuffle) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataIOHelper(data, worker.getProperties()).coalesce(numPartitions, shuffle);
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
    public IDataFrameId mapPartitions(IDataFrameId id, ISource src, boolean preservesPartitioning) throws IDriverException, TException {
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
    public IDataFrameId mapPartitionsWithIndex(IDataFrameId id, ISource src, boolean preservesPartitioning) throws IDriverException, TException {
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
    public IDataFrameId applyPartition(IDataFrameId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            synchronized (worker.getLock()) {
                IDataFrame result = new IDataGeneralHelper(data, worker.getProperties()).applyPartition(src);
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
    public long reduce(IDataFrameId id, ISource src, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).reduce(src, attributes.driver, tp);
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
                result = new IDataGeneralActionHelper(data, worker.getProperties()).treeReduce(src, attributes.driver, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long treeReduce4(IDataFrameId id, ISource src, long depth, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).treeReduce(src, depth, attributes.driver, tp);
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
    public long aggregate(IDataFrameId id, ISource seqOp, ISource combOp, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).aggregate(seqOp, combOp, attributes.driver, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long treeAggregate(IDataFrameId id, ISource seqOp, ISource combOp, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).treeAggregate(seqOp, combOp, attributes.driver, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long treeAggregate5(IDataFrameId id, ISource seqOp, ISource combOp, long depth, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).treeAggregate(seqOp, combOp, depth, attributes.driver, tp);
            }
            return result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long fold(IDataFrameId id, ISource src, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).fold(src, attributes.driver, tp);
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
                result = new IDataGeneralActionHelper(data, worker.getProperties()).take(num, attributes.driver, tp);
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
    public long top(IDataFrameId id, long num, ISource tp) throws IDriverException, TException {
        try {
            ICluster cluster = attributes.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            IDataFrame data = worker.getDataFrame(id.getDataFrame());
            ILazy<Long> result;
            synchronized (worker.getLock()) {
                result = new IDataGeneralActionHelper(data, worker.getProperties()).top(num, attributes.driver, tp);
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
                result = new IDataGeneralActionHelper(data, worker.getProperties()).top(num, cmp, attributes.driver);
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
    public long max(IDataFrameId id, ISource cmp, ISource tp) throws IDriverException, TException {
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
    public long min(IDataFrameId id, ISource cmp, ISource tp) throws IDriverException, TException {
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

}
