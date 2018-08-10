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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.helpers.IHelper;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IPropsKeys;
import org.ignis.backend.properties.IPropsParser;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IImportDataTask extends IExecutorTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IImportDataTask.class);

    public static final boolean SEND = false;
    public static final boolean RECEIVE = true;

    public static class Shared {

        //Executor -> Count (Multiple Write, One Read)
        private final Map<IExecutor, Long> count = new ConcurrentHashMap<>();

        private final Set<IExecutor> target = ConcurrentHashMap.newKeySet();

        //Source -> (Target -> Count) (One Write, Multiple Read)
        private final Map<IExecutor, SortedMap<IExecutor, Long>> msgs = new HashMap<>();
    }

    private final IBarrier barrier;
    private final Shared keyShared;
    private final boolean type;
    private final long parts;

    public IImportDataTask(IHelper helper, IExecutor executor, IBarrier barrier, Shared keyShared, boolean type, long parts) {
        super(helper, executor);
        this.barrier = barrier;
        this.keyShared = keyShared;
        this.type = type;
        this.parts = parts;
    }

    private void distribution() {
        //Algoritmo para decidir como se reparten los elementos TODO
    }

    @Override
    public void execute() throws IgnisException {
        try {
            if (type == SEND) {
                LOGGER.info(log() + "Counting elements");
                keyShared.count.put(executor, executor.getStorageModule().count());
                LOGGER.info(log() + keyShared.count.get(executor) + " elements");
            } else {
                keyShared.target.add(executor);
            }
            if (barrier.await() == 0) {
                LOGGER.info(log() + "Calculating element distribution");
                distribution();
            }
            barrier.await();
            if (type == SEND) {
                LOGGER.info(log() + "Creating " + keyShared.msgs.get(executor).size() + " partitions");
                executor.getShuffleModule().createSplits();
                int i = 1;
                int port = IPropsParser.getInteger(executor.getContainer().getProperties(), IPropsKeys.TRANSPORT_PORT);
                StringBuilder addr = new StringBuilder();
                for (Map.Entry<IExecutor, Long> msg : keyShared.msgs.get(executor).entrySet()) {
                    addr.setLength(0);
                    //TODO shared memory
                    if (msg.getKey() == executor) {
                        addr.append("local");
                    } else {
                        IContainer container = msg.getKey().getContainer();
                        addr.append("socket!").append(container.getHost()).append("!").append(port);
                    }
                    executor.getShuffleModule().nextSplit(addr.toString(), msg.getValue());
                    LOGGER.info(log() + "Partition  " + (i++) + "with " + msg.getValue() + " elements to " + addr.toString());
                }
                executor.getShuffleModule().finishSplits();
                LOGGER.info(log() + "Partitions created");
            }
            barrier.await();
            try {
                if (type == RECEIVE) {
                    LOGGER.info(log() + "Preparing to recive partitions");
                    executor.getPostmanModule().start();
                }
                barrier.await();
                if (type == SEND) {
                    LOGGER.info(log() + "Preparing to send partitions");
                    executor.getPostmanModule().sendAll();
                    LOGGER.info(log() + "Partitions sent");
                }
                barrier.await();
            } finally {
                if (type == RECEIVE) {
                    executor.getPostmanModule().stop();
                }
            }
            if (type == RECEIVE) {
                List<Long> order = keyShared.msgs.get(executor).keySet().stream().map(e -> e.getJob()).collect(Collectors.toList());
                LOGGER.info(log() + "Joining partitions");
                executor.getShuffleModule().joinSplits(order);
                LOGGER.info(log() + "Partitions joined");
            }
            barrier.await();
        } catch (IgnisException ex) {
            barrier.reset();
            throw ex;
        } catch (Exception ex) {
            barrier.reset();
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

}
