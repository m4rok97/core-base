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
import org.ignis.backend.cluster.IDriver;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.IWorker;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.executor.IParallelizeTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IWorkerParallelizeDataHelper extends IWorkerHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IWorkerParallelizeDataHelper.class);

    public IWorkerParallelizeDataHelper(IWorker worker, IProperties properties) {
        super(worker, properties);
    }

    public IDataFrame parallelize(IDriver driver, long dataId, long partitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newDependency(worker.getTasks());
        IParallelizeTask.Shared shared = new IParallelizeTask.Shared(worker.getExecutors().size());
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new IParallelizeTask(getName(), executor, shared, false, partitions));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnectionWithData(dataId));
        builder.newTask(new IParallelizeTask(driver.getName(), driver.getExecutor(), shared, true, partitions));

        IDataFrame target = worker.createDataFrame(worker.getExecutors(), builder.build());
        LOGGER.info(log() + "parallelize(" +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame parallelize(IDriver driver, long dataId, long partitions, ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newDependency(worker.getTasks());
        IParallelizeTask.Shared shared = new IParallelizeTask.Shared(worker.getExecutors().size());
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new IParallelizeTask(getName(), executor, shared, false, partitions, src));
        }
        builder.newLock(driver.getLock());
        builder.newDependency(driver.driverConnectionWithData(dataId));
        builder.newTask(new IParallelizeTask(driver.getName(), driver.getExecutor(), shared, true, partitions, src));

        IDataFrame target = worker.createDataFrame(worker.getExecutors(), builder.build());
        LOGGER.info(log() + "parallelize(" +
                "partitions=" + partitions +
                ") registered -> " + target.getName());
        return target;
    }
}
