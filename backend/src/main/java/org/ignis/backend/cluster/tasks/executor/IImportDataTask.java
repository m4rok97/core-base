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

import org.apache.thrift.TException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IExecutorExceptionWrapper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.IExecutorException;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

/**
 * @author César Pomar
 */
public final class IImportDataTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IImportDataTask.class);

    public static class Shared {

        public Shared(int sources, int targets, String commId, int threads) {
            this.commId = commId;
            this.threads= threads;
            barrier = new IBarrier(sources + targets);
        }

        private final IBarrier barrier;
        private final String commId;
        private final int threads;
        private String group;
        private boolean test;
    }

    private final Shared shared;
    private final boolean source;
    private final ISource src;

    private Integer attempt;

    public IImportDataTask(String name, IExecutor executor, Shared shared, boolean source, ISource src) {
        super(name, executor, source ? Mode.LOAD : Mode.SAVE);
        this.shared = shared;
        this.source = source;
        this.src = src;
        this.attempt = -1;
    }

    public IImportDataTask(String name, IExecutor executor, Shared shared, boolean source) {
        this(name, executor, shared, source, null);
    }

    @Override
    public void contextError(IgnisException ex) throws IgnisException {
        shared.barrier.fails();
        throw ex;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "importData started");
        try {
            prepareGroup();
            if(source || src == null){
                executor.getCommModule().importData(shared.commId, source, shared.threads);
            }else{
                executor.getCommModule().importData4(shared.commId, source, shared.threads, src);
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
        LOGGER.info(log() + "importData finished");
    }

    private void prepareGroup() throws BrokenBarrierException, InterruptedException, TException {
        LOGGER.info(log() + "testing mpi group between workers " + shared.commId);
        shared.test = true;
        shared.barrier.await();
        if (executor.getResets() != attempt) {
            shared.test = false;
        }
        shared.barrier.await();
        if (!shared.test) {
            if (attempt != -1) {
                executor.getCommModule().destroyGroup(shared.commId);
            }
            attempt = executor.getResets();
            if (source && executor.getId() == 0) {
                LOGGER.info(log() + "mpi group " + shared.commId + " not found, creating a new one");
                shared.group = executor.getCommModule().openGroup();
            }
            shared.barrier.await();
            executor.getCommModule().joinToGroupName(shared.group, source, shared.commId);
            if (source && executor.getId() == 0) {
                executor.getCommModule().closeGroup();
            }
        }
        LOGGER.info(log() + "mpi group " + shared.commId + " ready");
        shared.barrier.await();
    }

}
