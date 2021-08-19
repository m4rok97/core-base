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
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.ISSH;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.container.IContainerCreateTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.scheduler.IScheduler;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IClusterCreateHelper extends IClusterHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IClusterCreateHelper.class);

    public IClusterCreateHelper(ICluster cluster, IProperties properties) {
        super(cluster, properties);
    }

    public ITaskGroup create(IScheduler scheduler, ISSH ssh) throws IgnisException {
        int instances = properties.getInteger(IKeys.EXECUTOR_INSTANCES);
        if(instances < 1){
            throw new  IgnisException("Executor instances must be greater than zero");
        }
        ITaskGroup.Builder builder = new ITaskGroup.Builder(cluster.getLock());
        LOGGER.info(log() + "Registering cluster with " + instances + " containers");

        for (int i = 0; i < instances; i++) {
            IContainer container = new IContainer(i, cluster.getId(), ssh.createTunnel(), properties);
            cluster.getContainers().add(container);
        }

        IContainerCreateTask.Shared shared = new IContainerCreateTask.Shared(cluster.getContainers());
        for (int i = 0; i < instances; i++) {
            builder.newTask(new IContainerCreateTask(getName(), cluster.getContainers().get(i), scheduler, shared));
        }

        return builder.build();
    }

}
