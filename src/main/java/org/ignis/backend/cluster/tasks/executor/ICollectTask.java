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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.ignis.backend.cluster.IExecutionContext;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.helpers.IHelper;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class ICollectTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ITakeTask.class);

    public static class Shared {

        //Executor -> Result (Multiple Write, One Read)
        private final Map<IExecutor, ByteBuffer> result = new ConcurrentHashMap<>();

    }

    private final List<IExecutor> executors;
    private final IBarrier barrier;
    private final IExecutor driver;
    private final Shared shared;
    private final boolean ligth;

    public ICollectTask(IHelper helper, IExecutor executor, List<IExecutor> executors, IBarrier barrier, Shared shared,
            IExecutor driver, boolean ligth) {
        super(helper, executor, Mode.LOAD);
        this.executors = executors;
        this.barrier = barrier;
        this.shared = shared;
        this.driver = driver;
        this.ligth = ligth;
    }

    @Override
    public void execute(IExecutionContext context) throws IgnisException {
        try {
            if (barrier.await() == 0) {
                shared.result.clear();
                LOGGER.info(log() + "Executing " + (ligth ? "ligth " : "") + "collect");
            }
            barrier.await();
            ByteBuffer bytes = executor.getStorageModule().collect(executor.getId(), "none", ligth);//TODO
            if (ligth) {
                shared.result.put(executor, bytes);
            }
            barrier.await();
            if (ligth) {
                ligthMode(context);
            } else {
                directMode(context);
            }
            if (barrier.await() == 0) {
                LOGGER.info(log() + "Collect executed");
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

    private void ligthMode(IExecutionContext context) throws Exception {
        if (barrier.await() == 0) {
            int size = shared.result.values().stream().mapToInt(b -> b.capacity()).sum();
            ByteBuffer data = ByteBuffer.allocate(size);
            for (IExecutor e : executors) {
                data.put(shared.result.get(e));
            }
            context.set("result", data);
        }
    }

    private void directMode(IExecutionContext context) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");//TODO
    }

}
