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

        public Shared(int sources, int targets, String commId) {
            this.sources = sources;
            this.targets = targets;
            this.commId = commId;
            barrier = new IBarrier(sources + targets);
            count = new ArrayList<>(Collections.nCopies(sources, 0l));
            sends = new ArrayList<>();
            rcvs = new ArrayList<>();
        }

        private final int sources;
        private final int targets;
        private final IBarrier barrier;
        private final String commId;
        private final List<Long> count;
        private final List<Integer[]> sends;
        private final List<Integer[]> rcvs;
        private String group;
        private boolean test;
    }

    private final Shared shared;
    private final boolean source;
    private final Long partitions;
    private final ISource src;
    private Integer attempt;

    public IImportDataTask(String name, IExecutor executor, Shared shared, boolean source, Long partitions, ISource src) {
        super(name, executor, source ? Mode.LOAD : Mode.SAVE);
        this.shared = shared;
        this.source = source;
        this.partitions = partitions;
        this.src = src;
        this.attempt = -1;
    }

    public IImportDataTask(String name, IExecutor executor, Shared shared, boolean source, ISource src) {
        this(name, executor, shared, source, null, src);
    }

    public IImportDataTask(String name, IExecutor executor, Shared shared, boolean source, Long partitions) {
        this(name, executor, shared, source, partitions, null);
    }

    public IImportDataTask(String name, IExecutor executor, Shared shared, boolean source) {
        this(name, executor, shared, source, null, null);
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "importData started");
        int id = (int) executor.getId();
        try {
            if (source) {
                shared.count.set(id, executor.getIoModule().partitionCount());
            }
            prepareGroup();
            if (shared.barrier.await() == 0) {
                prepareMessages();
            }

            if (source) {
                for (int i = 0; i < shared.sends.size(); i++) {
                    if (shared.sends.get(i)[id] != null) {
                        executor.getCommModule().send(shared.commId, i, shared.sends.get(i)[id], 0);//TODO threading
                    }
                    shared.barrier.await();
                }
            } else {
                if (src == null) {
                    executor.getCommModule().newEmptyPartitions(shared.rcvs.size());
                } else {
                    executor.getCommModule().newEmptyPartitions2(shared.rcvs.size(), src);
                }
                for (int i = 0; i < shared.rcvs.size(); i++) {
                    if (shared.rcvs.get(i)[id] != null) {
                        executor.getCommModule().recv(shared.commId, i, shared.rcvs.get(i)[id], 0);//TODO threading
                    }
                    shared.barrier.await();
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
        LOGGER.info(log() + "importData finished");
    }

    private void prepareMessages() {
        long totalCount = shared.count.stream().reduce(0l, Long::sum);
        int maxIt = shared.count.stream().max(Long::compareTo).get().intValue();
        ArrayList<Long> targetCount = new ArrayList<>();
        targetCount.addAll(Collections.nCopies((int) (totalCount % shared.targets), totalCount / shared.targets + 1));
        targetCount.addAll(Collections.nCopies(shared.targets - targetCount.size(), totalCount / shared.targets));
        if (maxIt < targetCount.get(0)) {
            maxIt = targetCount.get(0).intValue();
        }
        shared.sends.clear();
        shared.rcvs.clear();
        for (int i = 0; i < maxIt; i++) {
            shared.sends.add(new Integer[shared.sources]);
            shared.rcvs.add(new Integer[shared.targets]);
        }

        int it = 0;
        int s_index = 0;
        int t_index = 0;
        long s_count = 0;
        long t_count = 0;

        for (long i = 0; i < totalCount; i++) {
            while (t_count == targetCount.get(t_index)) {
                t_index++;
                t_count = 0;
            }
            while (s_count == shared.count.get(s_index)) {
                s_index++;
                s_count = 0;
            }

            while (shared.sends.get(it)[s_index] == null && shared.rcvs.get(it)[t_index] == null) {
                it++;
                if (it == maxIt) {
                    it = 0;
                }
            }

            shared.sends.get(it)[s_index] = t_index;
            shared.rcvs.get(it)[t_index] = s_index;
        }
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
