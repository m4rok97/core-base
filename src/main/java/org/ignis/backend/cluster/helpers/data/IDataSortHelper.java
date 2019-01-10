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
package org.ignis.backend.cluster.helpers.data;

import java.util.ArrayList;
import java.util.List;
import org.ignis.backend.cluster.IData;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.cluster.tasks.ITaskScheduler;
import org.ignis.backend.cluster.tasks.executor.ISortTask;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IDataSortHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataSortHelper.class);

    public IDataSortHelper(IData data, IProperties properties) {
        super(data, properties);
    }

    public IData sort(boolean ascending) {
        List<IExecutor> result = new ArrayList<>();
        ITaskScheduler.Builder shedulerBuilder = new ITaskScheduler.Builder(data.getLock());
        shedulerBuilder.newDependency(data.getScheduler());
        int executors = data.getExecutors().size();
        ISortTask.Shared shared = new ISortTask.Shared();
        IBarrier barrier = new IBarrier(executors);
        for (IExecutor executor : data.getExecutors()) {
            shedulerBuilder.newTask(new ISortTask(this, executor, data.getExecutors(), barrier, shared, ascending));
            result.add(executor);
        }
        IData target = data.getJob().newData(result, shedulerBuilder.build());
        LOGGER.info(log() + "Sort " + (!ascending ? "descending " : "") + "-> " + target.getName());
        return target;
    }

    public IData sortBy(ISource function, boolean ascending) {
        List<IExecutor> result = new ArrayList<>();
        ITaskScheduler.Builder shedulerBuilder = new ITaskScheduler.Builder(data.getLock());
        shedulerBuilder.newDependency(data.getScheduler());
        int executors = data.getExecutors().size();
        ISortTask.Shared shared = new ISortTask.Shared();
        IBarrier barrier = new IBarrier(executors);
        for (IExecutor executor : data.getExecutors()) {
            shedulerBuilder.newTask(new ISortTask(this, executor, data.getExecutors(), barrier, shared, function, ascending));
            result.add(executor);
        }
        IData target = data.getJob().newData(result, shedulerBuilder.build());
        LOGGER.info(log() + "SortBy " + (!ascending ? "descending " : "") + "-> " + target.getName());
        return target;
    }

}
