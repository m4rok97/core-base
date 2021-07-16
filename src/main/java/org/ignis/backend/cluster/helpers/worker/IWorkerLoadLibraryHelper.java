/*
 * Copyright (C) 2021
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

import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.IWorker;
import org.ignis.backend.cluster.tasks.ICondicionalTaskGroup;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.executor.ILoadLibraryTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

public class IWorkerLoadLibraryHelper extends IWorkerHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IWorkerLoadLibraryHelper.class);

    public IWorkerLoadLibraryHelper(IWorker job, IProperties properties) {
        super(job, properties);
    }

    public ILazy<Void> loadLibrary(String path) throws IgnisException {
        LOGGER.info(log() + "Registering loadLibrary " + path);
        ITaskGroup.Builder builder = new ICondicionalTaskGroup.Builder(() -> worker.isRunning());
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new ILoadLibraryTask(getName(), executor, path));
        }
        ITaskGroup group = builder.build();
        worker.getTasks().getSubTasksGroup().add(group);

        return () -> {
            ITaskGroup dummy = new ITaskGroup.Builder(worker.getLock()).newDependency(group).build();
            dummy.start(worker.getPool());
            return null;
        };
    }
}
