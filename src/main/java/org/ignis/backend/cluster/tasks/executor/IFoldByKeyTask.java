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
package org.ignis.backend.cluster.tasks.executor;

import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IExecutorExceptionWrapper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.IExecutorException;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author César Pomar
 */
public final class IFoldByKeyTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IFoldByKeyTask.class);

    public static class Shared {

        public Shared(int executors) {
            this.executors = executors;
            barrier = new IBarrier(executors);
            partitions = new AtomicLong(0);
        }

        private final IBarrier barrier;
        private final int executors;
        AtomicLong partitions;

    }

    private final ISource zero;
    private final ISource src;
    private final Long numPartitions;
    private final boolean localFold;
    private final Shared shared;

    public IFoldByKeyTask(String name, IExecutor executor, Shared shared, ISource zero, ISource src, boolean localFold) {
        super(name, executor, Mode.LOAD_AND_SAVE);
        this.shared = shared;
        this.zero = zero;
        this.src = src;
        this.numPartitions = null;
        this.localFold = localFold;
    }

    public IFoldByKeyTask(String name, IExecutor executor, Shared shared, ISource zero, ISource src, long numPartitions, boolean localFold) {
        super(name, executor, Mode.LOAD_AND_SAVE);
        this.shared = shared;
        this.zero = zero;
        this.src = src;
        this.numPartitions = numPartitions;
        this.localFold = localFold;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "foldByKey started");
        try {
            if (numPartitions == null) {
                shared.partitions.set(0);
                shared.barrier.await();
                shared.partitions.addAndGet(executor.getIoModule().partitionCount());
                shared.barrier.await();
                executor.getGeneralModule().foldByKey(zero, src, shared.partitions.get(), localFold);
            } else {
                executor.getGeneralModule().foldByKey(zero, src, numPartitions, localFold);
            }
        } catch (IExecutorException ex) {
            shared.barrier.fails();
            throw new IExecutorExceptionWrapper(ex);
        } catch (BrokenBarrierException ex) {
            //Other Task has failed
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "foldByKey finished");
    }

}
