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
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.executor.IPartitionsCountTask;
import org.ignis.backend.cluster.tasks.executor.ISaveAsJsonFileTask;
import org.ignis.backend.cluster.tasks.executor.ISaveAsPartitionObjectFile;
import org.ignis.backend.cluster.tasks.executor.ISaveAsTextFileTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IDataIOHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataIOHelper.class);

    public IDataIOHelper(IDataFrame data, IProperties properties) {
        super(data, properties);
    }

    public IDataFrame repartition(long numPartitions) {
        throw new UnsupportedOperationException("Not supported on this version."); //TODO next version
    }

    public IDataFrame coalesce(long numPartitions, boolean shuffle) {
        throw new UnsupportedOperationException("Not supported on this version."); //TODO next version
    }

    public ILazy<Long> partitions() {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        LOGGER.info(log() + "Registering partitions count");
        IPartitionsCountTask.Shared shared = new IPartitionsCountTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IPartitionsCountTask(getName(), executor, shared));
        }
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Void> saveAsPartitionObjectFile(String path, byte compression) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        LOGGER.info(log() + "Registering saveAsPartitionObjectFile path: " + path + ", compression: " + compression);
        ISaveAsPartitionObjectFile.Shared shared = new ISaveAsPartitionObjectFile.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISaveAsPartitionObjectFile(getName(), executor, shared, path));
        }
        return () -> {
            builder.build().start(data.getPool());
            return null;
        };
    }

    public ILazy<Void> saveAsTextFile(String path) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        LOGGER.info(log() + "Registering saveAsTextFile path: " + path);
        ISaveAsTextFileTask.Shared shared = new ISaveAsTextFileTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISaveAsTextFileTask(getName(), executor, shared, path));
        }
        return () -> {
            builder.build().start(data.getPool());
            return null;
        };
    }

    public ILazy<Void> saveAsJsonFile(String path) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        LOGGER.info(log() + "Registering saveAsJsonFile path: " + path);
        ISaveAsJsonFileTask.Shared shared = new ISaveAsJsonFileTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISaveAsJsonFileTask(getName(), executor, shared, path));
        }
        return () -> {
            builder.build().start(data.getPool());
            return null;
        };
    }
}
