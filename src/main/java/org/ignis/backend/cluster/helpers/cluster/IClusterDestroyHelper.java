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
import org.ignis.backend.cluster.tasks.TaskScheduler;
import org.ignis.backend.cluster.tasks.container.IContainerDestroyTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IClusterDestroyHelper extends IClusterHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IClusterDestroyHelper.class);

    public IClusterDestroyHelper(ICluster cluster, IProperties properties) {
        super(cluster, properties);
    }

    public void destroy() throws IgnisException {
        LOGGER.info(log() + "Preparing cluster to destroy");
        TaskScheduler.Builder shedulerBuilder = new TaskScheduler.Builder(cluster.getLock());
        int instances = 0;
        for (IContainer container : cluster.getContainers()) {
            if (container.getStub().isRunning()) {
                instances++;
                shedulerBuilder.newTask(new IContainerDestroyTask(this, container));
            }
        }
        if (instances > 0) {
            LOGGER.info(log() + "Destroying " + instances + " instances");
            shedulerBuilder.build().execute(cluster.getPool());
        }
        LOGGER.info(log() + "Cluster destroyed");
    }

}
