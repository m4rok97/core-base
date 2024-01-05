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
import org.ignis.backend.cluster.tasks.executor.*;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IDataGeneralActionHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataGeneralActionHelper.class);

    public IDataGeneralActionHelper(IDataFrame data, IProperties properties) {
        super(data, properties);
    }

    public ILazy<Long> reduce(IDriver driver, ISource src, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IReduceTask.Shared shared = new IReduceTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IReduceTask(getName(), executor, shared, false, src, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new IReduceTask(driver.getName(), driver.getExecutor(), shared, true, src, tp));
        LOGGER.info(log() + "reduce(" +
                "zero=" + srcToString(src) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Long> treeReduce(IDriver driver, ISource src, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITreeReduceTask.Shared shared = new ITreeReduceTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITreeReduceTask(getName(), executor, shared, false, src, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new ITreeReduceTask(driver.getName(), driver.getExecutor(), shared, true, src, tp));
        LOGGER.info(log() + "treeReduce(" +
                "zero=" + srcToString(src) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
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
        builder.newDependency(driver.driverConnection());
        builder.newTask(new ICollectTask(driver.getName(), driver.getExecutor(), shared, true, tp));
        LOGGER.info(log() + "collect(" +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Long> aggregate(IDriver driver, ISource zero, ISource seqOp, ISource combOp, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IAggregateTask.Shared shared = new IAggregateTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IAggregateTask(getName(), executor, shared, false, zero, seqOp, combOp, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new IAggregateTask(driver.getName(), driver.getExecutor(), shared, true, zero, seqOp, combOp, tp));
        LOGGER.info(log() + "aggregate(" +
                "zero=" + srcToString(zero) +
                ", seqOp=" + srcToString(seqOp) +
                ", combOp=" + srcToString(combOp) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Long> treeAggregate(IDriver driver, ISource zero, ISource seqOp, ISource combOp, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITreeAggregateTask.Shared shared = new ITreeAggregateTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITreeAggregateTask(getName(), executor, shared, false, zero, seqOp, combOp, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new ITreeAggregateTask(driver.getName(), driver.getExecutor(), shared, true, zero, seqOp, combOp, tp));
        LOGGER.info(log() + "treeAggregate(" +
                "zero=" + srcToString(zero) +
                ", seqOp=" + srcToString(seqOp) +
                ", combOp=" + srcToString(combOp) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Long> fold(IDriver driver, ISource zero, ISource src, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IFoldTask.Shared shared = new IFoldTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IFoldTask(getName(), executor, shared, false, zero, src, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new IFoldTask(driver.getName(), driver.getExecutor(), shared, true, zero, src, tp));
        LOGGER.info(log() + "fold(" +
                "zero=" + srcToString(zero) +
                ", src=" + srcToString(src) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Long> treeFold(IDriver driver, ISource zero, ISource src, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IFoldTask.Shared shared = new IFoldTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITreeFoldTask(getName(), executor, shared, false, zero, src, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new ITreeFoldTask(driver.getName(), driver.getExecutor(), shared, true, zero, src, tp));
        LOGGER.info(log() + "treeFold(" +
                "zero=" + srcToString(zero) +
                ", src=" + srcToString(src) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Long> take(IDriver driver, long num, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITakeTask.Shared shared = new ITakeTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITakeTask(getName(), executor, shared, false, num, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new ITakeTask(driver.getName(), driver.getExecutor(), shared, true, num, tp));
        LOGGER.info(log() + "take(" +
                "num=" + num +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Void> foreach(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IForEachTask(getName(), executor, src));
        }
        LOGGER.info(log() + "foreach(" +
                "src=" + srcToString(src) +
                ") registered");
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
        LOGGER.info(log() + "foreachPartition(" +
                "src=" + srcToString(src) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return null;
        };
    }

    public ILazy<Void> foreachExecutor(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IForEachExecutorTask(getName(), executor, src));
        }
        LOGGER.info(log() + "foreachExecutor(" +
                "src=" + srcToString(src) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return null;
        };
    }

    public ILazy<Long> top(IDriver driver, long num, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITopTask.Shared shared = new ITopTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITopTask(getName(), executor, shared, false, num, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new ITopTask(driver.getName(), driver.getExecutor(), shared, true, num, tp));
        LOGGER.info(log() + "top(" +
                "num=" + num +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Long> top(IDriver driver, long num, ISource cmp, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITopTask.Shared shared = new ITopTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITopTask(getName(), executor, shared, false, num, cmp, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new ITopTask(driver.getName(), driver.getExecutor(), shared, true, num, cmp, tp));
        LOGGER.info(log() + "top(" +
                "num=" + num +
                ", cmp=" + srcToString(cmp) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Long> takeOrdered(IDriver driver, long num, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITakeOrderedTask.Shared shared = new ITakeOrderedTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITakeOrderedTask(getName(), executor, shared, false, num, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new ITakeOrderedTask(driver.getName(), driver.getExecutor(), shared, true, num, tp));
        LOGGER.info(log() + "takeOrdered(" +
                "num=" + num +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Long> takeOrdered(IDriver driver, long num, ISource cmp, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ITakeOrderedTask.Shared shared = new ITakeOrderedTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITakeOrderedTask(getName(), executor, shared, false, num, cmp, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new ITakeOrderedTask(driver.getName(), driver.getExecutor(), shared, true, num, cmp, tp));
        LOGGER.info(log() + "takeOrdered(" +
                "num=" + num +
                ", cmp=" + srcToString(cmp) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Long> keys(IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ICollectTask.Shared shared = new ICollectTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IKeysTask(getName(), executor, shared, false, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new IKeysTask(driver.getName(), driver.getExecutor(), shared, true, tp));
        LOGGER.info(log() + "keys(" +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

    public ILazy<Long> values(IDriver driver, ISource tp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ICollectTask.Shared shared = new ICollectTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IValuesTask(getName(), executor, shared, false, tp));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnection());
        builder.newTask(new IValuesTask(driver.getName(), driver.getExecutor(), shared, true, tp));
        LOGGER.info(log() + "values(" +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.popContext(driver.getExecutor());
        };
    }

}
