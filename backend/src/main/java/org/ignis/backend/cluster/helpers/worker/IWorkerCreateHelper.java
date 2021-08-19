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
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IWorkerCreateHelper extends IWorkerHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IWorkerCreateHelper.class);

    public IWorkerCreateHelper(IWorker worker, IProperties properties) {
        super(worker, properties);
    }

    public ITaskGroup create(int instances) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        ITaskGroup.Builder commBuilder = new ITaskGroup.Builder();
        int cores = worker.getCores();
        if(cores < 1){
            throw new  IgnisException("Executor cores must be greater than zero");
        }
        if (instances < 1) {
            if (worker.getProperties().getStringList(IKeys.EXECUTOR_CORES_SINGLE).contains(worker.getType())) {
                instances = cores;
            } else {
                instances = 1;
            }
        }
        int executors = worker.getCluster().getContainers().size() * instances;
        LOGGER.info(log() + "Registering worker with " + executors + " executors with " + cores + " cores");

        ICommGroupCreateTask.Shared commShared = new ICommGroupCreateTask.Shared(executors);

        builder.newDependency(worker.getCluster().getTasks());
        for (IContainer container : worker.getCluster().getContainers()) {
            for (int i = 0; i < instances; i++) {
                IExecutor executor = container.createExecutor(container.getId() * instances + i, worker.getId(), cores);
                builder.newTask(new IExecutorCreateTask(getName(), executor, worker.getType()));
                commBuilder.newTask(new ICommGroupCreateTask(getName(), executor, commShared));
                worker.getExecutors().add(executor);
            }
        }

        ITaskGroup taskGroup = builder.build();
        taskGroup.getSubTasksGroup().add(commBuilder.build());

        return taskGroup;
    }

}
