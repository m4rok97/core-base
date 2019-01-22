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
import java.util.stream.Collectors;
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
public class ICountTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ICountTask.class);

    public static class Shared {

        private final AtomicLong result = new AtomicLong();

    }

    private final Shared shared;
    private final IBarrier barrier;

    public ICountTask(IHelper helper, IExecutor executor, IBarrier barrier, Shared shared) {
        super(helper, executor, Mode.LOAD);
        this.barrier = barrier;
        this.shared = shared;
    }

    @Override
    public void execute(IExecutionContext context) throws IgnisException {      
        try {
            if (barrier.await() == 0) {
                shared.result.set(0);
                LOGGER.info(log() + "Executing count");
            }
            barrier.await();
            shared.result.addAndGet(executor.getStorageModule().count());
            if (barrier.await() == 0) {
                context.set("result", shared.result.get());
                LOGGER.info(log() + "Count executed");
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
