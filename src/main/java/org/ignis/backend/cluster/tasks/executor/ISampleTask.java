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
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class ISampleTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ISampleTask.class);

    public static class Shared {

        public Shared(int executors) {
            count = new ArrayList<>(Collections.nCopies(executors, 0l));
            barrier = new IBarrier(executors);
        }

        private final List<Long> count;
        private final IBarrier barrier;

    }

    private final Shared shared;
    private final boolean withReplacement;
    private final double fraction;
    private final int seed;

    public ISampleTask(String name, IExecutor executor, Shared shared, boolean withReplacement, double fraction, int seed) {
        super(name, executor, Mode.LOAD_AND_SAVE);
        this.shared = shared;
        this.withReplacement = withReplacement;
        this.fraction = fraction;
        this.seed = seed;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "Executing sample");
        try {
            shared.count.set((int) executor.getId(), executor.getMathModule().count());
            if (shared.barrier.await() == 0) {
                long elems = shared.count.stream().reduce(0l, Long::sum);
                long num = (long) Math.ceil(elems * fraction);
                if (!withReplacement && elems < num) {
                    throw new IgnisException("There are not enough elements");
                }
                ISampleTask.sample(context, shared.count, withReplacement, num, seed);
            }
            shared.barrier.await();
            executor.getMathModule().takeSample(withReplacement, shared.count.get((int) executor.getId()), seed);
            shared.barrier.await();
        } catch (IgnisException ex) {
            shared.barrier.fails();
            throw ex;
        } catch (BrokenBarrierException ex) {
            //Other Task has failed
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "Sample executed");
    }

    public static void sample(ITaskContext context, List<Long> countList, boolean withReplacement, long num, int seed) throws Exception {
        if (countList.size() == 1) {
            countList.set(0, num);
            return;
        }
        long[] count = new long[countList.size()];
        long[] elemsExecutor = new long[countList.size()];
        double[] probs = new double[countList.size()];
        long elems = countList.stream().reduce(0l, Long::sum);
        double totalProb = 0;
        for (int i = 0; i < countList.size(); i++) {
            count[i] = countList.get(i);
            probs[i] = ((double) count[i]) / elems;
            totalProb += probs[i];
        }
        Random rand = new Random(seed);
        long sample = Math.min(20 * countList.size(), num);
        double csum = 0;
        for (long i = 0; i < sample; i++) {
            double r = rand.nextDouble() * totalProb;
            for (int j = 0; j < probs.length; j++) {
                csum += probs[j];
                if (csum > r) {
                    elemsExecutor[j]++;
                    //Reduce probability
                    if (!withReplacement) {
                        count[j]--;
                        double newProb = ((double) count[j]) / elems;
                        totalProb -= probs[j] - newProb;
                        probs[j] = newProb;
                    }
                }
            }
        }

        //Large sizes
        if (sample != num) {
            long total = 0;
            double inc = (double) num / sample;
            //Increase sample rounding down
            for (int i = 0; i < elemsExecutor.length; i++) {
                long aux = Math.round(elemsExecutor[i] * inc);
                total += aux;
                elemsExecutor[i] = aux;
            }
            //if not exact
            for (long i = total; i < num;) {
                int r = rand.nextInt(elemsExecutor.length);
                if (elemsExecutor[r] == count[r]) {
                    continue;
                }
                i++;
                elemsExecutor[r]++;
            }
        }

        for (int i = 0; i < countList.size(); i++) {
            countList.set(i, elemsExecutor[i]);
        }
    }

}
