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
public final class IAggregateByKeyTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IAggregateByKeyTask.class);

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
    private final ISource seqOp;
    private final ISource combOp;
    private final Long numPartitions;
    private final Shared shared;

    public IAggregateByKeyTask(String name, IExecutor executor, Shared shared, ISource zero, ISource seqOp) {
        this(name, executor, shared, zero, seqOp, null);

    }

    public IAggregateByKeyTask(String name, IExecutor executor, Shared shared, ISource zero, ISource seqOp, ISource combOp) {
        this(name, executor, shared, zero, seqOp, combOp, null);
    }

    public IAggregateByKeyTask(String name, IExecutor executor, Shared shared, ISource zero, ISource seqOp, long numPartitions) {
        this(name, executor, shared, zero, seqOp, null, numPartitions);
    }

    public IAggregateByKeyTask(String name, IExecutor executor, Shared shared, ISource zero, ISource seqOp, ISource combOp, Long numPartitions) {
        super(name, executor, Mode.LOAD_AND_SAVE);
        this.shared = shared;
        this.zero = zero;
        this.seqOp = seqOp;
        this.combOp = combOp;
        this.numPartitions = numPartitions;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "aggregateByKey started");
        try {
            if (numPartitions == null) {
                shared.partitions.set(0);
                shared.barrier.await();
                shared.partitions.addAndGet(executor.getIoModule().partitionCount());
                shared.barrier.await();
                if (combOp != null) {
                    executor.getGeneralModule().aggregateByKey4(zero, seqOp, combOp, shared.partitions.get());
                } else {
                    executor.getGeneralModule().aggregateByKey(zero, seqOp, shared.partitions.get());
                }
            } else {
                if (combOp != null) {
                    executor.getGeneralModule().aggregateByKey4(zero, seqOp, combOp, numPartitions);
                } else {
                    executor.getGeneralModule().aggregateByKey(zero, seqOp, numPartitions);
                }
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
        LOGGER.info(log() + "aggregateByKey finished");
    }

}
