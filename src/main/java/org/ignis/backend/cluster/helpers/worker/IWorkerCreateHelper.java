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

import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.IWorker;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.executor.ICommGroupCreateTask;
import org.ignis.backend.cluster.tasks.executor.IExecutorCreateTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IWorkerCreateHelper extends IWorkerHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IWorkerCreateHelper.class);

    public IWorkerCreateHelper(IWorker job, IProperties properties) {
        super(job, properties);
    }

    public ITaskGroup create() throws IgnisException {
        if (worker.getProperties().getStringList(IKeys.EXECUTOR_CORES_SINGLE).contains(worker.getType())) {
            return createSingle();
        }

        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        ITaskGroup.Builder commBuilder = new ITaskGroup.Builder();
        int instances = worker.getCluster().getContainers().size();
        ICommGroupCreateTask.Shared commShared = new ICommGroupCreateTask.Shared(instances);

        builder.newDependency(worker.getCluster().getTasks());
        LOGGER.info(log() + "Registering worker with " + instances + " executors");
        for (IContainer container : worker.getCluster().getContainers()) {
            IExecutor executor = container.createExecutor(container.getId(), worker.getId());
            builder.newTask(new IExecutorCreateTask(getName(), executor, worker.getType(), worker.getCores()));
            commBuilder.newTask(new ICommGroupCreateTask(getName(), executor, commShared));
            worker.getExecutors().add(executor);
        }

        ITaskGroup taskGroup = builder.build();
        taskGroup.getSubTasksGroup().add(commBuilder.build());

        return taskGroup;
    }

    public ITaskGroup createSingle() throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        ITaskGroup.Builder commBuilder = new ITaskGroup.Builder();
        int cores = worker.getCores();
        int instances = worker.getCluster().getContainers().size() * cores;
        ICommGroupCreateTask.Shared commShared = new ICommGroupCreateTask.Shared(instances);

        builder.newDependency(worker.getCluster().getTasks());
        LOGGER.info(log() + "Registering worker with " + instances + " executors");
        for (IContainer container : worker.getCluster().getContainers()) {
            for (int i = 0; i < cores; i++) {
                IExecutor executor = container.createExecutor(container.getId() * cores + i, worker.getId());
                builder.newTask(new IExecutorCreateTask(getName(), executor, worker.getType(), 1));
                commBuilder.newTask(new ICommGroupCreateTask(getName(), executor, commShared));
                worker.getExecutors().add(executor);
            }
        }

        ITaskGroup taskGroup = builder.build();
        taskGroup.getSubTasksGroup().add(commBuilder.build());

        return taskGroup;
    }

}
