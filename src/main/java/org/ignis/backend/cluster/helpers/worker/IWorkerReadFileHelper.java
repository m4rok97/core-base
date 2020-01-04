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
package org.ignis.backend.cluster.helpers.worker;

import org.ignis.backend.cluster.IDataFrame;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.IWorker;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.executor.IPartitionObjectFileTask;
import org.ignis.backend.cluster.tasks.executor.ITextFileTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IWorkerReadFileHelper extends IWorkerHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IWorkerReadFileHelper.class);

    public IWorkerReadFileHelper(IWorker job, IProperties properties) {
        super(job, properties);
    }

    public IDataFrame textFile(String path) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newDependency(worker.getTasks());
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new ITextFileTask(getName(), executor, path));
        }
        IDataFrame target = worker.createDataFrame("", worker.getExecutors(), builder.build());
        LOGGER.info(log() + "Registering textFile path: " + path + " -> " + target.getName());
        return target;
    }

    public IDataFrame textFile(String path, long partitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newDependency(worker.getTasks());
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new ITextFileTask(getName(), executor, path, partitions));
        }
        IDataFrame target = worker.createDataFrame("", worker.getExecutors(), builder.build());
        LOGGER.info(log() + "Registering textFile path: " + path + ", partitions: " + partitions + " -> " + target.getName());
        return target;
    }

    public IDataFrame partitionObjectFile(String path) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newDependency(worker.getTasks());
        IPartitionObjectFileTask.Shared shared = new IPartitionObjectFileTask.Shared(worker.getExecutors().size());
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new IPartitionObjectFileTask(getName(), executor, shared, path));
        }
        IDataFrame target = worker.createDataFrame("", worker.getExecutors(), builder.build());
        LOGGER.info(log() + "Registering partitionObjectFile path: " + path + " -> " + target.getName());
        return target;
    }

    public IDataFrame partitionObjectFile(String path, ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newDependency(worker.getTasks());
        IPartitionObjectFileTask.Shared shared = new IPartitionObjectFileTask.Shared(worker.getExecutors().size());
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new IPartitionObjectFileTask(getName(), executor, shared, path, src));
        }
        IDataFrame target = worker.createDataFrame("", worker.getExecutors(), builder.build());
        LOGGER.info(log() + "Registering  partitionObjectFile: " + path + " -> " + target.getName());
        return target;
    }

}
