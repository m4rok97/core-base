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
import org.ignis.backend.cluster.tasks.executor.ICallTask;
import org.ignis.backend.cluster.tasks.executor.IVoidCallTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

public class IWorkerCallHelper extends IWorkerHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IWorkerCallHelper.class);

    public IWorkerCallHelper(IWorker worker, IProperties properties) {
        super(worker, properties);
    }

    private void setName(boolean r, ISource src, boolean arg) {
        if (src.getObj().isSetName()) {
            String name = src.getObj().getName();
            if (!r) {
                name += "V";
            }
            if (!arg) {
                name += "0";
            }
            src.getObj().setName(name);
        }
    }

    public ILazy<Void> voidCall(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newDependency(worker.getTasks());
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new IVoidCallTask(getName(), executor, src, false));
        }
        setName(false, src, false);
        LOGGER.info(log() + "voidCall(" +
                "src=" + srcToString(src) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(worker.getPool());
            return null;
        };
    }

    public ILazy<Void> voidCall(IDataFrame source, ISource src) throws IgnisException {
        if (!worker.equals(source.getWorker())) {
            source = new IWorkerImportDataHelper(worker, properties).importDataFrame(source, src);
        }
        ITaskGroup.Builder builder = new ITaskGroup.Builder(source.getLock());
        builder.newDependency(source.getTasks());
        for (IExecutor executor : source.getExecutors()) {
            builder.newTask(new IVoidCallTask(getName(), executor, src, true));
        }
        setName(false, src, true);
        LOGGER.info(log() + "voidCall(" +
                "src=" + srcToString(src) +
                ") registered");
        return () -> {
            ITaskContext context = builder.build().start(worker.getPool());
            return null;
        };
    }

    public IDataFrame call(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(worker.getLock());
        builder.newDependency(worker.getTasks());
        for (IExecutor executor : worker.getExecutors()) {
            builder.newTask(new ICallTask(getName(), executor, src, false));
        }
        IDataFrame target = worker.createDataFrame(worker.getExecutors(), builder.build());
        setName(true, src, false);
        LOGGER.info(log() + "call(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }

    public IDataFrame call(IDataFrame source, ISource src) throws IgnisException {
        if (!worker.equals(source.getWorker())) {
            source = new IWorkerImportDataHelper(worker, properties).importDataFrame(source, src);
        }
        ITaskGroup.Builder builder = new ITaskGroup.Builder(source.getLock());
        builder.newDependency(source.getTasks());
        for (IExecutor executor : source.getExecutors()) {
            builder.newTask(new ICallTask(getName(), executor, src, true));
        }
        IDataFrame target = source.createDataFrame(builder.build());
        setName(true, src, true);
        LOGGER.info(log() + "map(" +
                "src=" + srcToString(src) +
                ") registered -> " + target.getName());
        return target;
    }
}
