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
import org.ignis.backend.cluster.IDriver;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.executor.IAggregateTask;
import org.ignis.backend.cluster.tasks.executor.ICollectTask;
import org.ignis.backend.cluster.tasks.executor.IFoldTask;
import org.ignis.backend.cluster.tasks.executor.IForEachPartitionTask;
import org.ignis.backend.cluster.tasks.executor.IForEachTask;
import org.ignis.backend.cluster.tasks.executor.IReduceTask;
import org.ignis.backend.cluster.tasks.executor.ITakeTask;
import org.ignis.backend.cluster.tasks.executor.ITopTask;
import org.ignis.backend.cluster.tasks.executor.ITreeAggregateTask;
import org.ignis.backend.cluster.tasks.executor.ITreeReduceTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IDataGeneralActionHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataGeneralActionHelper.class);

    public IDataGeneralActionHelper(IDataFrame data, IProperties properties) {
        super(data, properties);
    }

    public ILazy<Long> reduce(ISource src, IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IReduceTask.Shared shared = new IReduceTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IReduceTask(getName(), executor, shared, false, src,tp));
        }
        builder.newLock(driver.getLock());
        builder.newTask(new IReduceTask(driver.getName(), driver.getExecutor(), shared, true, src,tp));

        LOGGER.info(log() + "Registering reduce");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> treeReduce(ISource src, IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITreeReduceTask.Shared shared = new ITreeReduceTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITreeReduceTask(getName(), executor, shared, false, src,tp));
        }
        builder.newLock(driver.getLock());
        builder.newTask(new ITreeReduceTask(driver.getName(), driver.getExecutor(), shared, true, src,tp));

        LOGGER.info(log() + "Registering treeReduce");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> treeReduce(ISource src, long depth, IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITreeReduceTask.Shared shared = new ITreeReduceTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITreeReduceTask(getName(), executor, shared, false, src, depth,tp));
        }
        builder.newLock(driver.getLock());
        builder.newTask(new ITreeReduceTask(driver.getName(), driver.getExecutor(), shared, true, src, depth,tp));

        LOGGER.info(log() + "Registering treeReduce");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> collect(IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ICollectTask.Shared shared = new ICollectTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ICollectTask(getName(), executor, shared, false, tp));
        }
        builder.newLock(driver.getLock());
        builder.newTask(new ICollectTask(driver.getName(), driver.getExecutor(), shared, true, tp));

        LOGGER.info(log() + "Registering collect");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> aggregate(ISource seqOp, ISource combOp, IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IAggregateTask.Shared shared = new IAggregateTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IAggregateTask(getName(), executor, shared, false, seqOp, combOp, tp));
        }
        builder.newLock(driver.getLock());
        builder.newTask(new IAggregateTask(driver.getName(), driver.getExecutor(), shared, true, seqOp, combOp, tp));

        LOGGER.info(log() + "Registering treeReduce");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> treeAggregate(ISource seqOp, ISource combOp, IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITreeAggregateTask.Shared shared = new ITreeAggregateTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITreeAggregateTask(getName(), executor, shared, false, seqOp, combOp, tp));
        }
        builder.newLock(driver.getLock());
        builder.newTask(new ITreeAggregateTask(driver.getName(), driver.getExecutor(), shared, true, seqOp, combOp, tp));

        LOGGER.info(log() + "Registering treeReduce");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> treeAggregate(ISource seqOp, ISource combOp, long depth, IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITreeAggregateTask.Shared shared = new ITreeAggregateTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITreeAggregateTask(getName(), executor, shared, false, seqOp, combOp, depth, tp));
        }
        builder.newLock(driver.getLock());
        builder.newTask(new ITreeAggregateTask(driver.getName(), driver.getExecutor(), shared, true, seqOp, combOp, depth, tp));

        LOGGER.info(log() + "Registering treeReduce");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> fold(ISource src, IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IFoldTask.Shared shared = new IFoldTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IFoldTask(getName(), executor, shared, false, src, tp));
        }
        builder.newLock(driver.getLock());
        builder.newTask(new IFoldTask(driver.getName(), driver.getExecutor(), shared, true, src, tp));

        LOGGER.info(log() + "Registering treeReduce");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> take(long num, IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITakeTask.Shared shared = new ITakeTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITakeTask(getName(), executor, shared, false, num, tp));
        }
        builder.newLock(driver.getLock());
        builder.newTask(new ITakeTask(driver.getName(), driver.getExecutor(), shared, true, num, tp));

        LOGGER.info(log() + "Registering treeReduce");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Void> foreach(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IForEachTask(getName(), executor, src));
        }

        LOGGER.info(log() + "Registering foreach");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return null;
        };
    }

    public ILazy<Void> foreachPartition(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IForEachPartitionTask(getName(), executor, src));
        }

        LOGGER.info(log() + "foreachPartition foreach");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return null;
        };
    }

    public ILazy<Long> top(long num, IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITopTask.Shared shared = new ITopTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITopTask(getName(), executor, shared, false, num, tp));
        }
        builder.newLock(driver.getLock());
        builder.newTask(new ITopTask(driver.getName(), driver.getExecutor(), shared, true, num, tp));

        LOGGER.info(log() + "Registering treeReduce");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> top(long num, ISource cmp, IDriver driver) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITopTask.Shared shared = new ITopTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITopTask(getName(), executor, shared, false, num, cmp));
        }
        builder.newLock(driver.getLock());
        builder.newTask(new ITopTask(driver.getName(), driver.getExecutor(), shared, true, num, cmp));

        LOGGER.info(log() + "Registering treeReduce");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }
}
