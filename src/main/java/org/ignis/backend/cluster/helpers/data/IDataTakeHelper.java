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

import java.nio.ByteBuffer;
import org.ignis.backend.cluster.IData;
import org.ignis.backend.cluster.IExecutionContext;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ITaskScheduler;
import org.ignis.backend.cluster.tasks.executor.ITakeSampleTask;
import org.ignis.backend.cluster.tasks.executor.ITakeTask;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class IDataTakeHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataTakeHelper.class);

    public IDataTakeHelper(IData data, IProperties properties) {
        super(data, properties);
    }

    public ILazy<ByteBuffer> take(long n, boolean light) {
        IBarrier barrier = new IBarrier(data.getPartitions());
        ITakeTask.Shared shared = new ITakeTask.Shared();
        ITaskScheduler.Builder shedulerBuilder = new ITaskScheduler.Builder(data.getLock());
        shedulerBuilder.newDependency(data.getScheduler());
        for (IExecutor executor : data.getExecutors()) {
            shedulerBuilder.newTask(new ITakeTask(this, executor, data.getExecutors(), barrier, shared, null, n, light));
        }
        LOGGER.info(log() + "Take n: " + n + ", light: " + light);
        return () -> {
            IExecutionContext context = shedulerBuilder.build().execute(data.getPool());
            LOGGER.info(log() + "Take Done");
            return context.<ByteBuffer>get("result");
        };
    }

    public ILazy<ByteBuffer> takeSample(long n, boolean withRemplacement, int seed, boolean light) {
        IBarrier barrier = new IBarrier(data.getPartitions());
        ITakeSampleTask.Shared shared = new ITakeSampleTask.Shared();
        ITaskScheduler.Builder shedulerBuilder = new ITaskScheduler.Builder(data.getLock());
        shedulerBuilder.newDependency(data.getScheduler());
        for (IExecutor executor : data.getExecutors()) {
            shedulerBuilder.newTask(new ITakeSampleTask(this, executor, data.getExecutors(), barrier, shared, null,
                    n, withRemplacement, seed, light));
        }
        LOGGER.info(log() + "TakeSample n: " + n + ", withRemplacement: " + withRemplacement + ", seed: " + seed
                + ", light: " + light);
        return () -> {
            IExecutionContext context = shedulerBuilder.build().execute(data.getPool());
            LOGGER.info(log() + "TakeSample Done");
            return context.<ByteBuffer>get("result");
        };
    }

}
