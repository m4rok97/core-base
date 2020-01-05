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

import java.io.File;
import java.util.concurrent.BrokenBarrierException;
import org.apache.commons.io.filefilter.AbstractFileFilter;
import org.apache.thrift.TException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IExecutorExceptionWrapper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.IExecutorException;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public abstract class IPartitionFileTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IPartitionFileTask.class);

    public static class Shared {

        public Shared(int executors) {
            this.executors = executors;
            barrier = new IBarrier(executors);
        }

        private long partitionCount;
        private final IBarrier barrier;
        private final long executors;
    }

    private final Shared shared;
    private final String path;
    private final ISource src;

    public IPartitionFileTask(String name, IExecutor executor, Shared shared, String path) {
        this(name, executor, shared, path, null);
    }

    public IPartitionFileTask(String name, IExecutor executor, Shared shared, String path, ISource src) {
        super(name, executor, Mode.SAVE);
        this.shared = shared;
        this.path = path;
        this.src = src;
    }

    @Override
    public final void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "Executing partitionFile");
        try {
            if (shared.barrier.await() == 0) {
                File directory = new File(path);
                shared.partitionCount = directory.list(new AbstractFileFilter() {
                    @Override
                    public boolean accept(final File dir, final String name) {
                        return name.matches("part-[0-9]+(\\..*)?");
                    }
                }).length;

            }
            shared.barrier.await();
            long executorPartitions = shared.partitionCount / shared.executors;
            long remainder = shared.partitionCount % shared.executors;
            long first = executorPartitions * executor.getId();
            if (remainder < executor.getId()) {
                executorPartitions++;
                first += executor.getId();
            } else {
                first += remainder;
            }
            if (src != null) {
                read(path, first, executorPartitions, src);
            } else {
                read(path, first, executorPartitions);
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
        LOGGER.info(log() + "PartitionFile executed");
    }

    public abstract void read(String path, long first, long partitions, ISource src) throws IExecutorException, TException;

    public abstract void read(String path, long first, long partitions) throws IExecutorException, TException;

}
