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
package org.ignis.backend.cluster.helpers.job;

import java.util.ArrayList;
import java.util.List;
import org.ignis.backend.allocator.IExecutorStub;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.IJob;
import org.ignis.backend.cluster.tasks.ITaskScheduler;
import org.ignis.backend.cluster.tasks.executor.IExecutorCreateTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IJobCreateHelper extends IJobHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IJobCreateHelper.class);

    public IJobCreateHelper(IJob job, IProperties properties) {
        super(job, properties);
    }

    public List<IExecutor> create() throws IgnisException {
        List<IExecutor> result = new ArrayList<>();
        ITaskScheduler.Builder sheduleBuilder = new ITaskScheduler.Builder(job.getLock());
        LOGGER.info(log() + "Registering job with " + job.getCluster().getContainers().size() + " executors");
        for (IContainer container : job.getCluster().getContainers()) {
            IExecutorStub stub = new IExecutorStub(job.getId(), job.getType(), container, properties);
            IExecutor executor = container.createExecutor(job.getId(), stub);
            sheduleBuilder.newTask(new IExecutorCreateTask(this, executor));
            sheduleBuilder.newDependency(job.getCluster().getScheduler());
            result.add(executor);
        }
        job.putScheduler(sheduleBuilder.build());
        return result;
    }

}
