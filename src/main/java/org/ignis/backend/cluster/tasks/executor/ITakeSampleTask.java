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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class ITakeSampleTask extends IDriverTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ITakeSampleTask.class);

    public static class Shared extends IDriverTask.Shared {

        public Shared(int executors) {
            super(executors);
            count = new ArrayList<>(Collections.nCopies(executors, 0l));
        }

        private final List<Long> count;

    }

    private final Shared shared;
    private final boolean withReplacement;
    private final long num;
    private final int seed;

    public ITakeSampleTask(String name, IExecutor executor, Shared shared, boolean driver, boolean withReplacement, long num, int seed, ISource tp) {
        super(name, executor, shared, driver, tp);
        this.shared = shared;
        this.withReplacement = withReplacement;
        this.num = num;
        this.seed = seed;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "Executing takeSample");
        try {
            if (!driver) {
                shared.count.set((int) executor.getId(), executor.getMathModule().count());
            }
            if (driver && !withReplacement) {
                long elems = shared.count.stream().reduce(0l, Long::sum);
                if (elems < num) {
                    throw new IgnisException("There are not enough elements");
                }
            }
            if (driver) {
                ISampleTask.sample(context, shared.count, withReplacement, num, seed);
            }
            shared.barrier.await();
            if (!driver) {
                executor.getMathModule().takeSample(withReplacement, shared.count.get((int) executor.getId()), seed);
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
        LOGGER.info(log() + "TakeSample executed");
    }

}
