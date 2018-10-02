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
import org.ignis.backend.cluster.tasks.TaskScheduler;
import org.ignis.backend.cluster.tasks.executor.IImportDataTask;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IDataShuffleHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataShuffleHelper.class);

    public IDataShuffleHelper(IData data, IProperties properties) {
        super(data, properties);
    }

    public IData shuffle() {
        LOGGER.info(log() + "Preparing shuffle");
        List<IExecutor> result = new ArrayList<>();
        TaskScheduler.Builder shedulerBuilder = new TaskScheduler.Builder(data.getLock());
        shedulerBuilder.newDependency(data.getScheduler());
        int executors = data.getExecutors().size();

        IBarrier barrier = new IBarrier(executors);
        IImportDataTask.Shared shared = new IImportDataTask.Shared();
        for (IExecutor executor : data.getExecutors()) {
            shedulerBuilder.newTask(new IImportDataTask(this, executor, barrier, shared, IImportDataTask.SHUFFLE, 
                    data.getExecutors(), result));
            result.add(executor);
        }
        IData target = data.getJob().newData(result, shedulerBuilder.build());
        LOGGER.info(log() + "Shuffle -> " + target.toString());
        return target;
    }

}
