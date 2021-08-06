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
import org.ignis.backend.cluster.tasks.executor.IPartitionsTask;
import org.ignis.backend.cluster.tasks.executor.ISaveAsJsonFileTask;
import org.ignis.backend.cluster.tasks.executor.ISaveAsObjectFile;
import org.ignis.backend.cluster.tasks.executor.ISaveAsTextFileTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IDataIOHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataIOHelper.class);

    public IDataIOHelper(IDataFrame data, IProperties properties) {
        super(data, properties);
    }

    public ILazy<Long> partitions() {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        IPartitionsTask.Shared shared = new IPartitionsTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IPartitionsTask(getName(), executor, shared));
        }
        LOGGER.info(log() + "partitions(" +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Void> saveAsObjectFile(String path, byte compression) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ISaveAsObjectFile.Shared shared = new ISaveAsObjectFile.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISaveAsObjectFile(getName(), executor, shared, path, compression));
        }
        LOGGER.info(log() + "partitions(" +
                "path=" + path +
                "compression=" + compression +
                ") registered");
        return () -> {
            builder.build().start(data.getPool());
            return null;
        };
    }

    public ILazy<Void> saveAsTextFile(String path) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ISaveAsTextFileTask.Shared shared = new ISaveAsTextFileTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISaveAsTextFileTask(getName(), executor, shared, path));
        }
        LOGGER.info(log() + "partitions(" +
                "path=" + path +
                ") registered");
        return () -> {
            builder.build().start(data.getPool());
            return null;
        };
    }

    public ILazy<Void> saveAsJsonFile(String path, boolean pretty) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        ISaveAsJsonFileTask.Shared shared = new ISaveAsJsonFileTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISaveAsJsonFileTask(getName(), executor, shared, path, pretty));
        }
        LOGGER.info(log() + "saveAsJsonFile(" +
                "path=" + path +
                "pretty=" + pretty +
                ") registered");
        return () -> {
            builder.build().start(data.getPool());
            return null;
        };
    }
}
