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

import org.ignis.backend.cluster.IDataFrame;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.IWorker;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.executor.IExecuteTask;
import org.ignis.backend.cluster.tasks.executor.IExecuteToTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

public class IWorkerExecuteHelper extends IWorkerHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IWorkerExecuteHelper.class);

    public IWorkerExecuteHelper(IWorker worker, IProperties properties) {
        super(worker, properties);
    }

    public ILazy<Void> execute(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newDependency(worker.getTasks());
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new IExecuteTask(getName(), executor, src));
        }
        LOGGER.info(log() + "execute(" +
                "src=" + srcToString(src) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(worker.getPool());
            return null;
        };
    }

    public IDataFrame executeTo(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newDependency(worker.getTasks());
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new IExecuteToTask(getName(), executor, src));
        }
        IDataFrame target = worker.createDataFrame(worker.getExecutors(), builder.build());
        LOGGER.info(log() + "executeTo(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }
}
