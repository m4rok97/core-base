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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import org.ignis.backend.cluster.IAddrManager;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.IExecutionContext;
import org.ignis.backend.cluster.helpers.IHelper;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.ISource;
import org.ignis.rpc.executor.IExecutorKeys;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IReduceByKeyTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IReduceByKeyTask.class);

    public static class Shared {

        //Executor -> Key -> Count (Multiple Write, One Read)
        private final Map<IExecutor, List<Long>> count = new ConcurrentHashMap<>();
        //Key -> Executor (One write, Multiple read)
        private final Map<IExecutor, Map<IExecutor, List<Long>>> msgs = new HashMap<>();
    }

    private final ISource function;
    private final IBarrier barrier;
    private final Shared shared;
    private final boolean single;

    public IReduceByKeyTask(IHelper helper, IExecutor executor, ISource function, IBarrier barrier, Shared shared) {
        super(helper, executor, Mode.LOAD_AND_SAVE);
        this.function = function;
        this.barrier = barrier;
        this.shared = shared;
        this.single = barrier.getParties() == 1;
    }

    private void keyDistribution() {
        Map<Long, Set<IExecutor>> keys = new HashMap<>();// Inverse of Shared.count
        Map<IExecutor, Long> load = new HashMap<>();//Number of keys assigned to each executor
        for (Map.Entry<IExecutor, List<Long>> executorWithKeys : shared.count.entrySet()) {
            for (long key : executorWithKeys.getValue()) {
                Set<IExecutor> list = keys.get(key);
                if (list == null) {
                    keys.put(key, list = new HashSet<>());
                }
                list.add(executorWithKeys.getKey());
            }
            load.put(executorWithKeys.getKey(), 0l);
            shared.msgs.put(executorWithKeys.getKey(), new HashMap<>());
        }

        long loadFactor = keys.size() / (load.size() * 10) + 1;
        long maxKeys = loadFactor;
        for (Map.Entry<Long, Set<IExecutor>> keyInExecutors : keys.entrySet()) {
            Iterator<IExecutor> targets = keyInExecutors.getValue().iterator();
            while (targets.hasNext()) {
                IExecutor target = targets.next();
                long eload = load.get(target);
                if (eload == maxKeys) {
                    if (targets.hasNext()) {
                        continue;
                    }
                    maxKeys += loadFactor;
                    targets = keyInExecutors.getValue().iterator();
                }

                for (IExecutor source : keyInExecutors.getValue()) {
                    List<Long> list = shared.msgs.get(source).get(target);
                    if (list == null) {
                        shared.msgs.get(source).put(target, list = new ArrayList<>());
                    }
                    list.add(keyInExecutors.getKey());
                }
                load.put(target, eload + 1);
                break;
            }
        }
        /*
        for (Map.Entry<IExecutor, Map<IExecutor, List<Long>>> e1 : shared.msgs.entrySet()) {
            System.out.println(e1.getKey().getContainer().getId() + ":");
            for (Map.Entry<IExecutor, List<Long>> e2 : e1.getValue().entrySet()) {
                System.out.println("\t" + e2.getKey().getContainer().getId() + " -> " + e2.getValue().toString());
            }
        }*/
    }

    @Override
    public void execute(IExecutionContext context) throws IgnisException {
        try {
            if (barrier.await() == 0) {
                shared.count.clear();
                shared.msgs.clear();
                LOGGER.info(log() + "Executing reduceByKey");
            }
            barrier.await();
            LOGGER.info(log() + "Reducing executor keys");
            executor.getKeysModule().reduceByKey(function);
            LOGGER.info(log() + "Executor keys reduced");
            barrier.await();
            if (single) {
                LOGGER.info(log() + "Avoiding key exchange");
            } else {
                LOGGER.info(log() + "Preparing keys");
                List<Long> keys = executor.getKeysModule().getKeys();
                LOGGER.info(log() + "Keys ready");
                shared.count.put(executor, keys);
                LOGGER.info(log() + keys.size() + " keys");
                if (barrier.await() == 0) {
                    LOGGER.info(log() + "Calculating key distribution");
                    keyDistribution();
                }
                barrier.await();

                LOGGER.info(log() + "Preparing keys to send to " + shared.msgs.get(executor).size() + " executors");
                IAddrManager addrManager = new IAddrManager();
                List<IExecutorKeys> executorKeys = new ArrayList<>();
                for (Map.Entry<IExecutor, List<Long>> entry : shared.msgs.get(executor).entrySet()) {
                    String addr = addrManager.parseAddr(executor, entry.getKey());
                    executorKeys.add(new IExecutorKeys(executor.getId(), addr, entry.getValue()));
                    LOGGER.info(log() + entry.getValue().size() + " keys prepared to " + addr);
                }
                executor.getKeysModule().prepareKeys(executorKeys);
                try {
                    barrier.await();
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
                LOGGER.info(log() + "Joining keys");
                executor.getKeysModule().collectKeys();
                LOGGER.info(log() + "Reducing keys");
                executor.getKeysModule().reduceByKey(function);
            }
            LOGGER.info(log() + "Keys Reduced");
            if (barrier.await() == 0) {
                LOGGER.info(log() + "ReduceByKey Executed");
            }
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
