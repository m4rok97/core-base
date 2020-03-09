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
public final class ICommGroupCreateTask extends IExecutorTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ICommGroupCreateTask.class);

    public static class Shared {

        private boolean test;
        private String group;
        private final IBarrier barrier;
        private final int executors;

        public Shared(int executors) {
            this.executors = executors;
            barrier = new IBarrier(executors);
        }
    }

    private final Shared shared;
    private Integer attempt;

    public ICommGroupCreateTask(String name, IExecutor executor, Shared shared) {
        super(name, executor);
        this.shared = shared;
        this.attempt = executor.getResets() - 1;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        if(shared.executors == 1){
            return;
        }
        LOGGER.info(log() + "Testing mpi group");
        try {
            shared.test = true;
            shared.barrier.await();
            if (executor.getResets() != attempt) {
                shared.test = false;
            }
            shared.barrier.await();
            if (shared.test) {
                LOGGER.info(log() + "Executor mpi group ready");
                return;
            }
            if (attempt != null) {
                executor.getCommModule().destroyGroups();
            }
            attempt = executor.getResets();
            if (executor.getId() == 0) {
                LOGGER.info(log() + "Mpi driver group not found, creating a new one");
                shared.group = executor.getCommModule().createGroup();
            }
            shared.barrier.await();
            executor.getCommModule().joinGroupMembers(shared.group, executor.getId(), shared.executors);
            LOGGER.info(log() + "Executor mpi group ready");
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
    }

}
