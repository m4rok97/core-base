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

import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.IJob;
import org.ignis.backend.cluster.tasks.ITaskScheduler;
import org.ignis.backend.cluster.tasks.executor.IExecutorDestroyTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IJobDestroyHelper extends IJobHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IJobDestroyHelper.class);

    public IJobDestroyHelper(IJob job, IProperties properties) {
        super(job, properties);
    }

    public void destroy() throws IgnisException {
        LOGGER.info(log() + "Preparing cluster to destroy");
        ITaskScheduler.Builder shedulerBuilder = new ITaskScheduler.Builder(job.getLock());
        int instances = 0;
        for (IExecutor executor : job.getExecutors()) {
            if (executor.getStub().isRunning()) {
                instances++;
                shedulerBuilder.newTask(new IExecutorDestroyTask(this, executor));
            }
        }
        if (instances > 0) {
            LOGGER.info(log() + "Destroying " + instances + " instances");
            shedulerBuilder.build().execute(job.getPool());
        }
        LOGGER.info(log() + "Cluster destroyed");
    }

}
