/*
 * Copyright (C) 2019 César Pomar
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
package org.ignis.backend.cluster.helpers.dataframe;

import org.ignis.backend.cluster.IDataFrame;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.helpers.worker.IWorkerImportDataHelper;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.executor.*;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IDataGeneralHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataGeneralHelper.class);

    public IDataGeneralHelper(IDataFrame data, IProperties properties) {
        super(data, properties);
    }

    public IDataFrame map(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMapTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "map(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame filter(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IFilterTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "filter(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame flatmap(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IFlatmapTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "flatmap(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame keyBy(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IKeyByTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "keyBy(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame mapWithIndex(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMapWithIndexTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "mapWithIndex(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame mapPartitions(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMapPartitionsTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "mapPartitions(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame mapPartitionsWithIndex(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMapPartitionsWithIndexTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "mapPartitionsWithIndex(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame mapExecutor(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMapExecutorTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "mapExecutor(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame mapExecutorTo(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMapExecutorToTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "mapExecutorTo(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame groupBy(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IGroupByTask.Shared shared = new IGroupByTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IGroupByTask(getName(), executor, shared, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "groupBy(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame groupBy(ISource src, long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IGroupByTask.Shared shared = new IGroupByTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IGroupByTask(getName(), executor, shared, src, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "groupBy(" +
                "src=" + srcToString(src) +
                ", numPartitions=" + numPartitions +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame sort(boolean ascending) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortTask(getName(), executor, ascending));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "sort(" +
                "ascending=" + ascending +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame sort(boolean ascending, long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortTask(getName(), executor, ascending, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "sort(" +
                "ascending=" + ascending +
                ", numPartitions=" + numPartitions +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame sortBy(ISource src, boolean ascending) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortTask(getName(), executor, src, ascending));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "sortBy(" +
                "src=" + srcToString(src) +
                ", ascending=" + ascending +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame union(IDataFrame other, boolean preserveOrder) throws IgnisException {
        String otherName = other.getName();
        if (!data.getWorker().equals(other.getWorker())) {
            other = new IWorkerImportDataHelper(data.getWorker(), properties).importDataFrame(other);
        }
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        IUnionTask.Shared shared = new IUnionTask.Shared(data.getExecutors().size());
        builder.newDependency(other.getTasks());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IUnionTask(getName(), executor, shared, preserveOrder));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "union(" +
                "other=" + otherName +
                ", preserveOrder=" + preserveOrder +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame union(IDataFrame other, boolean preserveOrder, ISource src) throws IgnisException {
        String otherName = other.getName();
        if (!data.getWorker().equals(other.getWorker())) {
            other = new IWorkerImportDataHelper(data.getWorker(), properties).importDataFrame(other);
        }
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        IUnionTask.Shared shared = new IUnionTask.Shared(data.getExecutors().size());
        builder.newDependency(other.getTasks());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IUnionTask(getName(), executor, shared, preserveOrder, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "union(" +
                "other=" + otherName +
                ", preserveOrder=" + preserveOrder +
                ", src=" + srcToString(src) +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame join(IDataFrame other) throws IgnisException {
        String otherName = other.getName();
        if (!data.getWorker().equals(other.getWorker())) {
            other = new IWorkerImportDataHelper(data.getWorker(), properties).importDataFrame(other);
        }
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        IJoinTask.Shared shared = new IJoinTask.Shared(data.getExecutors().size());
        builder.newDependency(other.getTasks());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IJoinTask(getName(), executor, shared));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "join(" +
                "other=" + otherName +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame join(IDataFrame other, long numPartitions) throws IgnisException {
        String otherName = other.getName();
        if (!data.getWorker().equals(other.getWorker())) {
            other = new IWorkerImportDataHelper(data.getWorker(), properties).importDataFrame(other);
        }
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        IJoinTask.Shared shared = new IJoinTask.Shared(data.getExecutors().size());
        builder.newDependency(other.getTasks());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IJoinTask(getName(), executor, shared, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "join(" +
                "other=" + otherName +
                ", numPartitions=" + numPartitions +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame join(IDataFrame other, ISource src) throws IgnisException {
        String otherName = other.getName();
        if (!data.getWorker().equals(other.getWorker())) {
            other = new IWorkerImportDataHelper(data.getWorker(), properties).importDataFrame(other);
        }
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        IJoinTask.Shared shared = new IJoinTask.Shared(data.getExecutors().size());
        builder.newDependency(other.getTasks());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IJoinTask(getName(), executor, shared, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "join(" +
                "other=" + otherName +
                ", src=" + srcToString(src) +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame join(IDataFrame other, long numPartitions, ISource src) throws IgnisException {
        String otherName = other.getName();
        if (!data.getWorker().equals(other.getWorker())) {
            other = new IWorkerImportDataHelper(data.getWorker(), properties).importDataFrame(other);
        }
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        IJoinTask.Shared shared = new IJoinTask.Shared(data.getExecutors().size());
        builder.newDependency(other.getTasks());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IJoinTask(getName(), executor, shared, numPartitions, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "join(" +
                "other=" + otherName +
                ", numPartitions=" + numPartitions +
                ", src=" + srcToString(src) +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame distinct() throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        IDistinctTask.Shared shared = new IDistinctTask.Shared(data.getExecutors().size());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IDistinctTask(getName(), executor, shared));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "distinct(" +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame distinct(long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        IDistinctTask.Shared shared = new IDistinctTask.Shared(data.getExecutors().size());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IDistinctTask(getName(), executor, shared, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "distinct(" +
                "numPartitions=" + numPartitions +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame distinct(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        IDistinctTask.Shared shared = new IDistinctTask.Shared(data.getExecutors().size());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IDistinctTask(getName(), executor, shared, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "distinct(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame distinct(long numPartitions, ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        IDistinctTask.Shared shared = new IDistinctTask.Shared(data.getExecutors().size());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IDistinctTask(getName(), executor, shared, numPartitions, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "distinct(" +
                "numPartitions=" + numPartitions +
                ", src=" + srcToString(src) +
                ") registered -> " + target.getName()
        );
        return target;
    }


    public IDataFrame sortBy(ISource src, boolean ascending, long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortTask(getName(), executor, src, ascending, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "sortBy(" +
                "src=" + srcToString(src) +
                ", ascending=" + ascending +
                ", numPartitions=" + numPartitions +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame flatMapValues(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IFlatmapValuesTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "flatMapValues(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame mapValues(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMapValuesTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "mapValues(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame repartition(long numPartitions, boolean preserveOrdering, boolean global) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IRepartitionTask(getName(), executor, numPartitions, preserveOrdering, global));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "repartition(" +
                "numPartitions=" + numPartitions +
                ", preserveOrdering=" + preserveOrdering +
                ", global=" + global +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame partitionByRandom(long numPartitions, int seed) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IPartitionByRandomTask(getName(), executor, numPartitions, seed));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "partitionByRandom(" +
                "numPartitions=" + numPartitions +
                "seed=" + seed +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame partitionByHash(long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IPartitionByHashTask(getName(), executor, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "partitionByHash(" +
                "numPartitions=" + numPartitions +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame partitionBy(ISource src, long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IPartitionByTask(getName(), executor, src, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "partitionBy(" +
                "src=" + srcToString(src) +
                ", numPartitions=" + numPartitions +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame groupByKey() throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IGroupByKeyTask.Shared shared = new IGroupByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IGroupByKeyTask(getName(), executor, shared));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "groupByKey(" +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame groupByKey(long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IGroupByKeyTask.Shared shared = new IGroupByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IGroupByKeyTask(getName(), executor, shared, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "groupByKey(" +
                "numPartitions=" + numPartitions +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame groupByKey(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IGroupByKeyTask.Shared shared = new IGroupByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IGroupByKeyTask(getName(), executor, shared, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "groupByKey(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame groupByKey(long numPartitions, ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IGroupByKeyTask.Shared shared = new IGroupByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IGroupByKeyTask(getName(), executor, shared, numPartitions, src));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "groupByKey(" +
                "numPartitions=" + numPartitions +
                ", src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame reduceByKey(ISource src, boolean localReduce) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IReduceByKeyTask.Shared shared = new IReduceByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IReduceByKeyTask(getName(), executor, shared, src, localReduce));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "reduceByKey(" +
                "src=" + srcToString(src) +
                ", localReduce=" + localReduce +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame reduceByKey(ISource src, long numPartitions, boolean localReduce) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IReduceByKeyTask.Shared shared = new IReduceByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IReduceByKeyTask(getName(), executor, shared, src, numPartitions, localReduce));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "reduceByKey(" +
                "src=" + srcToString(src) +
                ", numPartitions=" + numPartitions +
                ", localReduce=" + localReduce +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame aggregateByKey(ISource zero, ISource seqOp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IAggregateByKeyTask.Shared shared = new IAggregateByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IAggregateByKeyTask(getName(), executor, shared, zero, seqOp));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "aggregateByKey(" +
                "zero=" + srcToString(zero) +
                ", seqOp=" + srcToString(seqOp) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame aggregateByKey(ISource zero, ISource seqOp, long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IAggregateByKeyTask.Shared shared = new IAggregateByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IAggregateByKeyTask(getName(), executor, shared, zero, seqOp, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "aggregateByKey(" +
                "zero=" + srcToString(zero) +
                ", seqOp=" + srcToString(seqOp) +
                ", numPartitions=" + numPartitions +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame aggregateByKey(ISource zero, ISource seqOp, ISource combOp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IAggregateByKeyTask.Shared shared = new IAggregateByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IAggregateByKeyTask(getName(), executor, shared, zero, seqOp, combOp));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "aggregateByKey(" +
                "zero=" + srcToString(zero) +
                ", seqOp=" + srcToString(seqOp) +
                ", combOp=" + srcToString(combOp) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame aggregateByKey(ISource zero, ISource seqOp, ISource combOp, long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IAggregateByKeyTask.Shared shared = new IAggregateByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IAggregateByKeyTask(getName(), executor, shared, zero, seqOp, combOp, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "aggregateByKey(" +
                "zero=" + srcToString(zero) +
                ", seqOp=" + srcToString(seqOp) +
                ", combOp=" + srcToString(combOp) +
                ", numPartitions=" + numPartitions +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame foldByKey(ISource zero, ISource src, boolean localFold) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IFoldByKeyTask.Shared shared = new IFoldByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IFoldByKeyTask(getName(), executor, shared, zero, src, localFold));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "foldByKey(" +
                "zero=" + srcToString(zero) +
                ", src=" + srcToString(src) +
                ", localFold=" + localFold +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame foldByKey(ISource zero, ISource src, long numPartitions, boolean localFold) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IFoldByKeyTask.Shared shared = new IFoldByKeyTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IFoldByKeyTask(getName(), executor, shared, zero, src, numPartitions, localFold));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "foldByKey(" +
                "zero=" + srcToString(zero) +
                ", src=" + srcToString(src) +
                ", numPartitions=" + numPartitions +
                ", localFold=" + localFold +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame sortByKey(boolean ascending) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortByKeyTask(getName(), executor, ascending));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "sortByKey(" +
                "ascending=" + ascending +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame sortByKey(boolean ascending, long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortByKeyTask(getName(), executor, ascending, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "sortByKey(" +
                "ascending=" + ascending +
                ", numPartitions=" + numPartitions +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame sortByKey(ISource src, boolean ascending) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortByKeyTask(getName(), executor, src, ascending));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "sortByKey(" +
                "src=" + srcToString(src) +
                ", ascending=" + ascending +
                ") registered -> " + target.getName()
        );
        return target;
    }

    public IDataFrame sortByKey(ISource src, boolean ascending, long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortByKeyTask(getName(), executor, src, ascending, numPartitions));
        }
        IDataFrame target = data.createDataFrame(builder.build());
        LOGGER.info(log() + "sortByKey(" +
                "src=" + srcToString(src) +
                ", ascending=" + ascending +
                ", numPartitions=" + numPartitions +
                ") registered -> " + target.getName()
        );
        return target;
    }


}
