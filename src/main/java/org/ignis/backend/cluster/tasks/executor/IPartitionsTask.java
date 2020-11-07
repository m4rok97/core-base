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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicLong;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IExecutorExceptionWrapper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.IExecutorException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class IPartitionsTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IPartitionsTask.class);

    public static class Shared {

        public Shared(int executors) {
            result = new AtomicLong();
            barrier = new IBarrier(executors);
        }

        private final AtomicLong result;
        private final IBarrier barrier;

    }

    private final Shared shared;

    public IPartitionsTask(String name, IExecutor executor, Shared shared) {
        super(name, executor, Mode.LOAD);
        this.shared = shared;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "partitions started");
        try {
            if (shared.barrier.await() == 0) {
                shared.result.set(0);
            }
            shared.result.addAndGet(executor.getIoModule().partitionCount());
            if (shared.barrier.await() == 0) {
                context.<Long>set("result", shared.result.get());
            }
            shared.barrier.await();
        } catch (IExecutorException ex) {
            shared.barrier.fails();
            throw new IExecutorExceptionWrapper(ex);
        } catch (BrokenBarrierException ex) {
            //Other Task has failed
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "partitions finished");
    }

}
