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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.thrift.TException;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.helpers.IHelper;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IPropsKeys;
import org.ignis.rpc.ISourceFunction;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IReduceByKeyTask extends IExecutorTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IReduceByKeyTask.class);

    public static class Shared {

        //Executor -> Key -> Count (Multiple Write, One Read)
        private final Map<IExecutor, Map<Long, Long>> count = new ConcurrentHashMap<>();
        //Key -> Executor (One write, Multiple read)
        private final Map<Long, IExecutor> distribution = new HashMap<>();
    }

    private final ISourceFunction function;
    private final IBarrier barrier;
    private final Shared keyShared;
    private final boolean single;

    public IReduceByKeyTask(IHelper helper, IExecutor executor, ISourceFunction function, IBarrier barrier, Shared keyShared) {
        super(helper, executor);
        this.function = function;
        this.barrier = barrier;
        this.keyShared = keyShared;
        this.single = barrier.getParties() == 1;
    }

    private void keyDistribution() {
        //Algotimo para decidir quien se queda con cada clave TODO
    }

    @Override
    public void execute() throws IgnisException {
        try {
            LOGGER.info(log() + "Reducing executor keys");
            executor.getReducerModule().reduceByKey(function);
            LOGGER.info(log() + "Executor keys reduced");
            barrier.await();
            LOGGER.info(log() + "Preparing keys");
            Map<Long, Long> keys = executor.getKeysModule().getKeys(single);
            LOGGER.info(log() + "Keys ready");
            if (single) {
                LOGGER.info(log() + "Avoiding key exchange");
            } else {
                keyShared.count.put(executor, keys);
                if (barrier.await() == 0) {
                    LOGGER.info(log() + "Calculating key distribution");
                    keyDistribution();
                }
                barrier.await();
                Map<IExecutor, List<Long>> messages = new HashMap<>();
                for (Long key : keys.values()) {
                    IExecutor to = keyShared.distribution.get(key);
                    if (to != null) {
                        List<Long> toKeys = messages.get(to);
                        if (toKeys == null) {
                            messages.put(to, toKeys = new ArrayList<>());
                        }
                        toKeys.add(key);
                    }
                }

                int port = executor.getContainer().getProperties().getInteger(IPropsKeys.TRANSPORT_PORT);

                LOGGER.info(log() + "Preparing keys to send to " + messages.size() + " executors");
                int i = 1;
                StringBuilder addr = new StringBuilder();
                for (Map.Entry<IExecutor, List<Long>> entry : messages.entrySet()) {                 
                    addr.setLength(0);
                    //TODO shared memory
                    IContainer container = entry.getKey().getContainer();
                    addr.append("socket!").append(container.getHost()).append("!").append(port);
                    executor.getKeysModule().sendPairs(addr.toString(), entry.getValue());
                    LOGGER.info(log() + (i++) + " prepared with " + entry.getValue().size() + " keys to " + addr.toString());
                }
                try {
                    LOGGER.info(log() + "Preparing to recive keys");
                    executor.getPostmanModule().start();
                    barrier.await();
                    LOGGER.info(log() + "Preparing to send keys");
                    executor.getPostmanModule().sendAll();
                    LOGGER.info(log() + "Keys sent");
                    barrier.await();
                } finally {
                    executor.getPostmanModule().stop();
                }
            }
            LOGGER.info(log() + "Joining keys");
            executor.getKeysModule().joinPairs();
            LOGGER.info(log() + "Reducing keys");
            executor.getReducerModule().reduceByKey(function);
            LOGGER.info(log() + "Keys Reduced");
            barrier.await();
        } catch (IgnisException ex) {
            barrier.reset();
            throw ex;
        } catch (Exception ex) {
            barrier.reset();
            throw new IgnisException(ex.getMessage(), ex);
        } finally {
            try {
                executor.getKeysModule().reset();
            } catch (TException ex) {
                throw new IgnisException(ex.getMessage(), ex);
            }
        }
    }

}
