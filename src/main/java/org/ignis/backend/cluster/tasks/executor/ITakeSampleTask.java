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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
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
public class ITakeSampleTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ITakeSampleTask.class);

    public static class Shared {

        //Executor -> Count (Multiple Write, One Read)
        private final Map<IExecutor, Long> count = new ConcurrentHashMap<>();
        //Executor -> Result (Multiple Write, One Read)
        private final Map<IExecutor, ByteBuffer> result = new ConcurrentHashMap<>();

    }

    private final List<IExecutor> executors;
    private final IBarrier barrier;
    private final IExecutor driver;
    private final Shared shared;
    private final long n;
    private final boolean withRemplacement;
    private final int seed;
    private final boolean ligth;

    public ITakeSampleTask(IHelper helper, IExecutor executor, List<IExecutor> executors, IBarrier barrier,
            ITakeSampleTask.Shared shared, IExecutor driver, long n, boolean withRemplacement, int seed, boolean ligth) {
        super(helper, executor, Mode.LOAD);
        this.executors = executors;
        this.barrier = barrier;
        this.driver = driver;
        this.shared = shared;
        this.n = n;
        this.withRemplacement = withRemplacement;
        this.seed = seed;
        this.ligth = ligth;
    }

    @Override
    public void execute(IExecutionContext context) throws IgnisException {
        try {
            if (barrier.await() == 0) {
                shared.count.clear();
                shared.result.clear();
                LOGGER.info(log() + "Executing " + (ligth ? "ligth " : "") + "takeSample");
            }
            barrier.await();
            shared.count.put(executor, executor.getStorageModule().count());
            if (barrier.await() == 0 && !withRemplacement) {
                long elems = shared.count.values().stream().reduce(0l, Long::sum);
                if (elems > n) {
                    throw new IgnisException("There are not enough elements");
                }
            }
            if (barrier.await() == 0) {
                sample(context);
            }
            barrier.await();
            long nExecutor = shared.count.get(executor);
            ByteBuffer bytes = executor.getStorageModule()
                    .takeSample(executor.getId(), "none", nExecutor, withRemplacement, seed, ligth);//TODO
            if (ligth) {
                shared.result.put(executor, null);
            }
            barrier.await();
            if (ligth) {
                ligthMode(context);
            } else {
                directMode(context);
            }
            if (barrier.await() == 0) {
                LOGGER.info(log() + "TakeSample executed");
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

    private void sample(IExecutionContext context) throws Exception {
        if (executors.size() == 1) {
            shared.count.put(executor, n);
            return;
        }
        long[] count = new long[executors.size()];
        long[] elemsExecutor = new long[executors.size()];
        double[] probs = new double[executors.size()];
        long elems = shared.count.values().stream().reduce(0l, Long::sum);
        double totalProb = 0;
        for (int i = 0; i < executors.size(); i++) {
            count[i] = shared.count.get(executors.get(i));
            probs[i] = ((double) shared.count.get(executors.get(i))) / elems;
            totalProb += probs[i];
        }
        Random rand = new Random(seed);
        long sample = Math.min(20 * executors.size(), n);
        double csum = 0;
        for (long i = 0; i < sample; i++) {
            double r = rand.nextDouble() * totalProb;
            for (int j = 0; j < probs.length; j++) {
                csum += probs[j];
                if (csum > r) {
                    elemsExecutor[j]++;
                    //Reduce probability
                    if (!withRemplacement) {
                        count[j]--;
                        double aux = 1;
                        double newProb = ((double) count[j]) / elems;
                        totalProb -= probs[j] - newProb;
                        probs[j] = newProb;
                    }
                }
            }
        }

        //Large sizes
        if (sample != n) {
            long total = 0;
            double inc = (double) n / sample;
            //Increase sample rounding down
            for (int i = 0; i < elemsExecutor.length; i++) {
                long aux = Math.round(elemsExecutor[i] * inc);
                total += aux;
                elemsExecutor[i] = aux;
            }
            //if not exact
            for (long i = total; i < n;) {
                int r = rand.nextInt(elemsExecutor.length);
                if (elemsExecutor[r] == count[r]) {
                    continue;
                }
                i++;
                elemsExecutor[r]++;
            }
        }
        for (int i = 0; i < executors.size(); i++) {
            shared.count.put(executors.get(i), elemsExecutor[i]);
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
