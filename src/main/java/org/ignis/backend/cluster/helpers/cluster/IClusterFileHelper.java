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
import org.ignis.backend.cluster.tasks.ICondicionalTaskGroup;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.container.ISendCompressedFileTask;
import org.ignis.backend.cluster.tasks.container.ISendFileTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IClusterFileHelper extends IClusterHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IClusterFileHelper.class);

    public IClusterFileHelper(ICluster cluster, IProperties properties) {
        super(cluster, properties);
    }

    public ILazy<Void> sendFile(String source, String target) throws IgnisException {
        LOGGER.info(log() + "Registering sendfile from " + source + " to " + target);
        ITaskGroup.Builder builder = new ICondicionalTaskGroup.Builder(() -> cluster.isRunning());
        for (IContainer container : cluster.getContainers()) {
            builder.newTask(new ISendFileTask(getName(), container, source, target));
        }
        ITaskGroup group = builder.build();
        cluster.getTasks().getSubTasksGroup().add(group);

        return () -> {
            ITaskGroup dummy = new ITaskGroup.Builder(cluster.getLock()).newDependency(group).build();
            dummy.start(cluster.getPool());
            return null;
        };
    }

    public ILazy<Void> sendCompressedFile(String source, String target) throws IgnisException {
        LOGGER.info(log() + "Registering sendCompressedFile file from " + source + " to " + target);
        ITaskGroup.Builder builder = new ICondicionalTaskGroup.Builder(() -> cluster.isRunning());
        for (IContainer container : cluster.getContainers()) {
            builder.newTask(new ISendCompressedFileTask(getName(), container, source, target));
        }
        ITaskGroup group = builder.build();
        cluster.getTasks().getSubTasksGroup().add(group);

        return () -> {
            ITaskGroup dummy = new ITaskGroup.Builder(cluster.getLock()).newDependency(group).build();
            dummy.start(cluster.getPool());
            return null;
        };
    }

}
