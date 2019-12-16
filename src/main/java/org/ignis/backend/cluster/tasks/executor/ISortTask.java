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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import org.ignis.backend.cluster.IAddrManager;
import org.ignis.backend.cluster.IExecutionContext;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.helpers.IHelper;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IKeys;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class ISortTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ISortTask.class);

    public static class Shared {

        //Executor -> Count (Multiple Write, One Read)
        private final Map<IExecutor, Long> count = new ConcurrentHashMap<>();

        private IExecutor master;
    }

    private final ISource function;
    private final List<IExecutor> executors;
    private final IBarrier barrier;
    private final Shared shared;
    private final boolean ascending;

    public ISortTask(IHelper helper, IExecutor executor, List<IExecutor> executors, IBarrier barrier, Shared shared,
            ISource function, boolean ascending) {
        super(helper, executor, Mode.LOAD_AND_SAVE);
        this.function = function;
        this.ascending = ascending;
        this.executors = executors;
        this.barrier = barrier;
        this.shared = shared;
    }

    public ISortTask(IHelper helper, IExecutor executor, List<IExecutor> executors, IBarrier barrier, Shared shared,
            boolean ascending) {
        this(helper, executor, executors, barrier, shared, null, ascending);
    }

    private void selectMaster() {
        Map<String, Integer> hosts = new HashMap<>();
        //Count containers hosts
        for (IExecutor e : executors) {
            String host = e.getContainer().getHost();
            hosts.put(host, hosts.getOrDefault(host, 0) + 1);
        }
        //Find best host
        String host = "";
        int max = 0;
        for (Map.Entry<String, Integer> entry : hosts.entrySet()) {
            if (entry.getValue() > max) {
                host = entry.getKey();
                max = entry.getValue();
            }
        }
        //Choose a Executor in best host
        for (IExecutor e : executors) {
            if (e.getContainer().getHost().equals(host)) {
                shared.master = e;
                return;
            }
        }
    }

    @Override
    public void execute(IExecutionContext context) throws IgnisException {
        try {
            LOGGER.info(log() + "Executing sort");
            if (barrier.await() == 0) {
                shared.count.clear();
            }
            if (executors.size() > 1) {
                LOGGER.info(log() + "Counting elements");
                shared.count.put(executor, executor.getStorageModule().count());
                barrier.await();
                LOGGER.info(log() + "Selecting a master");
                if (barrier.await() == 0) {
                    selectMaster();
                    LOGGER.info(log() + shared.master.getId() + " is the master");
                }
                LOGGER.info(log() + "Sorting local elements for sampling");
                if (function != null) {
                    executor.getSortModule().localCustomSort(function, ascending);
                } else {
                    executor.getSortModule().localSort(ascending);
                }
                barrier.await();
                long available = executor.getStorageModule().count();
                long elements = shared.count.values().stream().reduce(0l, (a, b) -> a + b);
                long sampleSize = (long) Math.ceil((executors.size() - 1)
                        * (1 + executor.getProperties().getDouble(IKeys.MODULES_SORT_SAMPLES)));
                long sampleExecutorSize = Math.min(
                        sampleSize * (long) Math.ceil(shared.count.get(executor) / (elements * 1.0)), available
                );
                LOGGER.info(log() + "Sampling " + sampleExecutorSize + " elements");
                IAddrManager addrManager = new IAddrManager();
                String addr = addrManager.parseAddr(executor, shared.master);
                executor.getSortModule().sampling(sampleExecutorSize, executor.getId(), addr);
                barrier.await();
                boolean contextSaved = false;
                try {
                    context.saveContext(executor);
                    contextSaved = true;
                    LOGGER.info(log() + "Preparing to recive data");
                    executor.getPostmanModule().start();
                    barrier.await();
                    LOGGER.info(log() + "Preparing to send samples");
                    executor.getPostmanModule().sendAll();
                    LOGGER.info(log() + "Samples sent");
                    barrier.await();
                    if (executor == shared.master) {
                        LOGGER.info(log() + "Merging samples");
                        executor.getSortModule().getPivots();
                        LOGGER.info(log() + "Finding pivots");
                        List<String> nodes = new ArrayList<>();
                        for (IExecutor target : executors) {
                            nodes.add(addrManager.parseAddr(executor, target));
                        }
                        executor.getSortModule().findPivots(nodes);
                        LOGGER.info(log() + "Sending pivots");
                        executor.getPostmanModule().sendAll();
                    }
                    barrier.await();
                    context.loadContext(executor);
                    contextSaved = false;
                    List<String> nodes = new ArrayList<>();
                    for (IExecutor target : executors) {
                        nodes.add(addrManager.parseAddr(executor, target));
                    }
                    LOGGER.info(log() + "Exchanging partitions");
                    executor.getSortModule().exchangePartitions(executor.getId(), nodes);
                    LOGGER.info(log() + "Sending partitions");
                    executor.getPostmanModule().sendAll();
                    LOGGER.info(log() + "Partitions sent");
                    barrier.await();
                } finally {
                    executor.getPostmanModule().stop();
                    if (contextSaved) {
                        context.removeContext(executor);
                    }
                }
                barrier.await();
                LOGGER.info(log() + "Merging partitions");
                executor.getSortModule().mergePartitions();
            }
            LOGGER.info(log() + "Sorting local elements");
            if (function != null) {
                executor.getSortModule().localCustomSort(function, ascending);
            } else {
                executor.getSortModule().localSort(ascending);
            }
            barrier.await();
            LOGGER.info(log() + "Sort executed");
        } catch (IgnisException ex) {
            barrier.fails();
            throw ex;
        } catch (BrokenBarrierException ex) {
            //Other Task has failed
        } catch (Exception ex) {
            barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

}
