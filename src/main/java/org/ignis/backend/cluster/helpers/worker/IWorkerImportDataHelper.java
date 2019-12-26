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
package org.ignis.backend.cluster.helpers.worker;

import org.ignis.backend.cluster.IDataFrame;
import org.ignis.backend.cluster.IWorker;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IWorkerImportDataHelper extends IWorkerHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IWorkerImportDataHelper.class);

    public IWorkerImportDataHelper(IWorker worker, IProperties properties) {
        super(worker, properties);
    }

    public IDataFrame importData(IDataFrame source) {
        /*LOGGER.info(log() + "Preparing importData");
        List<IExecutor> result = new ArrayList<>();
        ITaskGroup.Builder shedulerBuilder = new ITaskGroup.Builder(job.getLock());
        shedulerBuilder.newDependency(source.getScheduler());
        shedulerBuilder.newDependency(job.getScheduler());
        int senders = source.getExecutors().size();
        int receivers = job.getExecutors().size();

        IBarrier barrier = new IBarrier(senders + receivers);
        IImportDataTask.Shared shared = new IImportDataTask.Shared();
        IWorkerImportDataHelper sourceHelper = new IWorkerImportDataHelper(source.getJob(), source.getJob().getProperties());
        for (IExecutor executor : source.getExecutors()) {
            shedulerBuilder.newTask(new IImportDataTask(sourceHelper, executor, barrier, shared, IImportDataTask.SEND,
                    source.getExecutors(), result));
        }
        for (IExecutor executor : job.getExecutors()) {
            shedulerBuilder.newTask(new IImportDataTask(this, executor, barrier, shared, IImportDataTask.RECEIVE,
                    source.getExecutors(), result));
            result.add(executor);
        }
        IDataFrame target = job.createDataFrame(result, shedulerBuilder.build());
        LOGGER.info(log() + "ImportData -> " + target.getName());
        return target;*/
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public IDataFrame importData(IDataFrame source, ISource src) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
