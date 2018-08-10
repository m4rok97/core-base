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

import java.util.ArrayList;
import java.util.List;
import org.ignis.backend.allocator.IContainerStub;
import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.tasks.TaskScheduler;
import org.ignis.backend.cluster.tasks.container.IContainerCreateTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.properties.IPropsKeys;
import org.ignis.backend.properties.IPropsParser;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IClusterCreateHelper extends IClusterHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IClusterCreateHelper.class);

    public IClusterCreateHelper(ICluster cluster, IProperties properties) {
        super(cluster, properties);
    }

    public List<IContainer> create(IContainerStub.Factory factory) throws IgnisException {
        int instances = IPropsParser.getInteger(properties, IPropsKeys.EXECUTOR_INSTANCES);
        List<IContainer> result = new ArrayList<>();
        TaskScheduler.Builder shedulerBuilder = new TaskScheduler.Builder(cluster.getLock());
        LOGGER.info(log() + "Registering cluster with " + instances + " containers");
        for (int i = 0; i < instances; i++) {
            IContainerStub stub = factory.getContainerStub(properties);
            IContainer container = new IContainer(i, stub);
            shedulerBuilder.newTask(new IContainerCreateTask(this, container));
            result.add(container);
        }
        cluster.putScheduler(shedulerBuilder.build());
        return result;
    }

}
