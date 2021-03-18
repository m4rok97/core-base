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
package org.ignis.backend.cluster.helpers.cluster;

import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.cluster.helpers.worker.IWorkerDestroyHelper;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.container.IContainerDestroyTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.scheduler.IScheduler;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IClusterDestroyHelper extends IClusterHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IClusterDestroyHelper.class);

    public IClusterDestroyHelper(ICluster cluster, IProperties properties) {
        super(cluster, properties);
    }

    public ILazy<Void> destroy(IScheduler scheduler) {
        LOGGER.info(log() + "Preparing cluster to destroy");
        ITaskGroup.Builder builder = new ITaskGroup.Builder(cluster.getLock());
        if (!cluster.getContainers().isEmpty()) {// All containers are destroyed in single task, faster in some schedulers
            builder.newTask(new IContainerDestroyTask(getName(), cluster.getContainers().get(0), scheduler, cluster.getContainers()));
        }

        for (int i = 0; i < cluster.workers(); i++) {
            try {
                builder.newDependency(new IWorkerDestroyHelper(cluster.getWorker(i), cluster.getProperties()).destroy());
            } catch (IgnisException ex) {
            }
        }

        ITaskGroup target = builder.build();

        return () -> {
            LOGGER.info(log() + "Destroying cluster with " + cluster.getContainers().size() + " containers");
            target.start(cluster.getPool());
            LOGGER.info(log() + "Cluster destroyed");
            return null;
        };
    }

}
