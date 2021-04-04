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
import org.ignis.backend.cluster.tasks.executor.IImportDataTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IWorkerImportDataHelper extends IWorkerHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IWorkerImportDataHelper.class);

    public IWorkerImportDataHelper(IWorker worker, IProperties properties) {
        super(worker, properties);
    }

    public IDataFrame importDataFrame(IDataFrame source) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newLock(source.getLock());
        IImportDataTask.Shared shared = new IImportDataTask.Shared(source.getPartitions(), worker.getExecutors().size(),
                commId(source.getWorker(), worker));
        for (IExecutor executor : source.getExecutors()) {
            builder.newTask(new IImportDataTask(getName(), executor, shared, true));
        }
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new IImportDataTask(getName(), executor, shared, false));
        }
        IDataFrame target = worker.createDataFrame(worker.getExecutors(), builder.build());
        LOGGER.info(log() + "importDataFrame(" +
                "source=" + source.getName() +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame importDataFrame(IDataFrame source, ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newLock(source.getLock());
        IImportDataTask.Shared shared = new IImportDataTask.Shared(source.getPartitions(), worker.getExecutors().size(),
                commId(source.getWorker(), worker));
        for (IExecutor executor : source.getExecutors()) {
            builder.newTask(new IImportDataTask(getName(), executor, shared, true, src));
        }
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new IImportDataTask(getName(), executor, shared, false, src));
        }
        IDataFrame target = worker.createDataFrame(worker.getExecutors(), builder.build());
        LOGGER.info(log() + "importDataFrame(" +
                "source=" + source.getName() +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame importDataFrame(IDataFrame source, long partitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newLock(source.getLock());
        IImportDataTask.Shared shared = new IImportDataTask.Shared(source.getPartitions(), worker.getExecutors().size(),
                commId(source.getWorker(), worker));
        for (IExecutor executor : source.getExecutors()) {
            builder.newTask(new IImportDataTask(getName(), executor, shared, true, partitions));
        }
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new IImportDataTask(getName(), executor, shared, false, partitions));
        }
        IDataFrame target = worker.createDataFrame(worker.getExecutors(), builder.build());
        LOGGER.info(log() + "importDataFrame(" +
                "source=" + source.getName() +
                "partitions=" + partitions +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame importDataFrame(IDataFrame source, long partitions, ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newLock(source.getLock());
        IImportDataTask.Shared shared = new IImportDataTask.Shared(source.getPartitions(), worker.getExecutors().size(),
                commId(source.getWorker(), worker));
        for (IExecutor executor : source.getExecutors()) {
            builder.newTask(new IImportDataTask(getName(), executor, shared, true, partitions, src));
        }
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new IImportDataTask(getName(), executor, shared, false, partitions, src));
        }
        IDataFrame target = worker.createDataFrame(worker.getExecutors(), builder.build());
        LOGGER.info(log() + "importDataFrame(" +
                "source=" + source.getName() +
                "partitions=" + partitions +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    private String commId(IWorker w1, IWorker w2) {
        if (w1.getLock().compareTo(w2.getLock()) > 0) {
            IWorker aux = w1;
            w1 = w2;
            w2 = aux;
        }
        return w1.getCluster().getId() + "." + w1.getId() + "." + w2.getCluster().getId() + "." + w2.getId();
    }

}
