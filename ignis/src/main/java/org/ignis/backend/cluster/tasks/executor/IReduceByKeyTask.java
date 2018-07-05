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
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.cluster.tasks.Task;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IPropertiesKeys;
import org.ignis.backend.properties.IPropertiesParser;
import org.ignis.rpc.IFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class IReduceByKeyTask extends IExecutorTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(IReduceByKeyTask.class);
    
    public static class KeyShared {

        //Executor -> Key -> Count (Multiple Write, One Read)
        private final Map<IExecutor, Map<Long, Long>> count = new ConcurrentHashMap<>();
        //Key -> Executor (One write, Multiple read)
        private final Map<Long, IExecutor> distribution = new HashMap<>();
        //Executor -> id (One write, Multiple read)
        private final Map<IExecutor, Long> ids = new HashMap<>();
    }

    private final IFunction function;
    private final IBarrier barrier;
    private final KeyShared keyShared;

    public IReduceByKeyTask(IExecutor executor, IFunction function, IBarrier barrier, KeyShared keyShared, ILock lock, Task... dependencies) {
        super(executor, lock, dependencies);
        this.function = function;
        this.barrier = barrier;
        this.keyShared = keyShared;
    }

    private void keyDistribution() {
        //Algotimo para decidir quien se queda con cada clave TODO
    }

    @Override
    public void execute() throws IgnisException {
        try {
            Map<Long, Long> keys = executor.getReducerModule().getKeys(function, barrier.getParties() == 1);
            keyShared.count.put(executor, keys);
            if (barrier.await() == 0) {
                keyDistribution();
            }
            executor.getPostmanModule().start();
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

            int port = IPropertiesParser.getInteger(executor.getContainer().getProperties(),
                    IPropertiesKeys.EXECUTOR_TRANSPORT_PORT);

            for (Map.Entry<IExecutor, List<Long>> entry : messages.entrySet()) {
                IContainer container = entry.getKey().getContainer();
                executor.getReducerModule().setExecutorKeys(container.getHost(), container.getPortAlias(port),
                        entry.getValue(), keyShared.ids.get(executor));
            }
            executor.getPostmanModule().sendAll();
            barrier.await();
        } catch (IgnisException ex) {
            barrier.reset();
            throw ex;
        } catch (Exception ex) {
            barrier.reset();
            throw new IgnisException(ex.getMessage(), ex);
        } finally {
            try {
                executor.getPostmanModule().stop();
            } catch (TException ex) {
                //TODO
            }
        }
    }

}
