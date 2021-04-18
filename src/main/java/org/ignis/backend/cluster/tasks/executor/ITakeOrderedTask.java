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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

/**
 * @author César Pomar
 */
public class ITakeOrderedTask extends IDriverTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ITakeOrderedTask.class);

    public static class Shared extends IDriverTask.Shared {

        public Shared(int executors) {
            super(executors);
            count = new ArrayList<>(Collections.nCopies(executors, 0l));
        }

        private final List<Long> count;
        private boolean useSort;

    }

    private final Shared shared;
    private final long n;
    private final ISource src;

    public ITakeOrderedTask(String name, IExecutor executor, Shared shared, boolean driver, long n, ISource tp) {
        this(name, executor, shared, driver, n, null, tp);
    }

    public ITakeOrderedTask(String name, IExecutor executor, Shared shared, boolean driver, long n, ISource src, ISource tp) {
        super(name, executor, shared, driver, tp);
        this.n = n;
        this.shared = shared;
        this.src = src;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        try {
            LOGGER.info(log() + "takeOrdered started");
            if (!driver) {
                shared.count.set((int) executor.getId(), executor.getMathModule().count());
            }
            shared.barrier.await();
            if (driver) {
                long elems = shared.count.stream().reduce(0l, Long::sum);
                if (elems < n) {
                    throw new IgnisException("There are not enough elements");
                }
                shared.useSort = elems * elems > n;
                if (shared.useSort) {
                    long remainder = n;
                    for (int i = 0; i < shared.executors; i++) {
                        long localN = shared.count.get((int) executor.getId());
                        if (remainder >= localN) {
                            remainder -= localN;
                        } else {
                            shared.count.set((int) executor.getId(), remainder);
                            remainder = 0;
                        }
                    }
                }
            }
            shared.barrier.await();
            if (!driver) {
                if (shared.useSort) {
                    if (src != null) {
                        executor.getGeneralModule().sortBy(src, true);
                    } else {
                        executor.getGeneralModule().sort(true);
                    }
                    executor.getGeneralActionModule().take(shared.count.get((int) executor.getId()));
                } else {
                    if (src != null) {
                        executor.getGeneralActionModule().takeOrdered2(n, src);
                    } else {
                        executor.getGeneralActionModule().takeOrdered(n);
                    }
                }
            }
            shared.barrier.await();
            gather(context, true);
        } catch (IgnisException ex) {
            shared.barrier.fails();
            throw ex;
        } catch (BrokenBarrierException ex) {
            //Other Task has failed
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "takeOrdered finished");
    }

}
