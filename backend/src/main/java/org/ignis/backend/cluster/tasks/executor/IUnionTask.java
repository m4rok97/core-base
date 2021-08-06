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

/**
 * @author César Pomar
 */
public final class IUnionTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IUnionTask.class);

    public static class Shared {

        public Shared(int executors) {
            this.executors = executors;
            barrier = new IBarrier(executors);
        }

        private final IBarrier barrier;
        private final int executors;

    }

    private final ISource src;
    private final Shared shared;
    private final boolean preserveOrder;


    public IUnionTask(String name, IExecutor executor, Shared shared, boolean preserveOrder) {
        this(name, executor, shared, preserveOrder,  null);
    }


    public IUnionTask(String name, IExecutor executor, Shared shared, boolean preserveOrder, ISource src) {
        super(name, executor, Mode.LOAD_AND_SAVE);
        this.shared = shared;
        this.preserveOrder = preserveOrder;
        this.src = src;
    }

    @Override
    public void contextError(IgnisException ex) throws IgnisException {
        shared.barrier.fails();
        throw ex;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "union started");
        try {
            context.loadContextAsVariable(executor, "other");
            if (src == null) {
                executor.getGeneralModule().union_("other", preserveOrder);
            } else {
                executor.getGeneralModule().union2("other", preserveOrder, src);
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
        LOGGER.info(log() + "union finished");
    }

}
