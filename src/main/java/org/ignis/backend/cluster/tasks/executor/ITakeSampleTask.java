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

import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;

/**
 * @author César Pomar
 */
public class ITakeSampleTask extends IDriverTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ITakeSampleTask.class);

    public static class Shared extends IDriverTask.Shared {

        @SuppressWarnings("unchecked")
        public Shared(int executors) {
            super(executors);
            count = new List[executors];
        }

        private final List<Long>[] count;

    }

    private final Shared shared;
    private final boolean withReplacement;
    private final long num;
    private final int seed;

    public ITakeSampleTask(String name, IExecutor executor, Shared shared, boolean driver, boolean withReplacement, long num, int seed, ISource tp) {
        super(name, executor, driver ? Mode.SAVE : Mode.LOAD, shared, driver, tp);
        this.shared = shared;
        this.withReplacement = withReplacement;
        this.num = num;
        this.seed = seed;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "takeSample started");
        try {
            if (!driver) {
                shared.count[(int) executor.getId()] = executor.getIoModule().countByPartition();
            }
            if (shared.barrier.await() == 0) {
                long elems = 0;
                for (List<Long> l : shared.count) {
                    elems += l.stream().reduce(0l, Long::sum);
                }
                if (!withReplacement && elems < num) {
                    throw new IgnisException("There are not enough elements");
                }
                ISampleTask.sample(context, shared.count, withReplacement, num, elems, seed);
            }
            shared.barrier.await();
            if (!driver) {
                executor.getMathModule().sample(withReplacement, shared.count[(int) executor.getId()], seed);
            }
            shared.barrier.await();
            gather(context);
        } catch (IgnisException ex) {
            shared.barrier.fails();
            throw ex;
        } catch (BrokenBarrierException ex) {
            //Other Task has failed
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "takeSample finished");
    }

}
