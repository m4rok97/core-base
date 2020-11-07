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

import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.stream.Collectors;

import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public class ISampleTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ISampleTask.class);

    public static class Shared {

        public Shared(int executors) {
            count = new List[executors];
            barrier = new IBarrier(executors);
        }

        private final List<Long>[] count;
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
        LOGGER.info(log() + "sample started");
        try {
            shared.count[(int) executor.getId()] = executor.getIoModule().countByPartition();
            if (shared.barrier.await() == 0) {
                long elems = 0;
                for (List<Long> l : shared.count) {
                    elems += l.stream().reduce(0l, Long::sum);
                }
                long num = (long) Math.ceil(elems * fraction);
                if (!withReplacement && elems < num) {
                    throw new IgnisException("There are not enough elements");
                }
                sample(context, shared.count, withReplacement, num, elems, seed);
            }
            shared.barrier.await();
            executor.getMathModule().sample(withReplacement, shared.count[(int) executor.getId()], seed);
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
        LOGGER.info(log() + "sample executed");
    }

    public static void sample(ITaskContext context, List<Long>[] countListArray, boolean withReplacement, long num, long elems, int seed) throws Exception {
        if ((countListArray.length == 1 && countListArray[0].size() == 1) || num == 0) {
            countListArray[0].set(0, num);
            return;
        }
        int countListSize = Arrays.stream(countListArray).map(List::size).reduce(0, Integer::sum);
        long[] count = new long[countListSize];
        long[] elemsExecutor = new long[countListSize];
        double[] probs = new double[countListSize];
        double totalProb = 0;
        int global_i = 0;
        for (List<Long> countList : countListArray) {
            for (int i = 0; i < countList.size(); i++) {
                count[global_i] = countList.get(i);
                probs[global_i] = ((double) count[global_i]) / elems;
                totalProb += probs[global_i];
                global_i++;
            }
        }
        Random rand = new Random(seed);
        long sample = Math.min(20 * countListSize, num);
        double csum = 0;
        for (long i = 0; i < num; i++) {
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
            for (long i = total; i < num; ) {
                int r = rand.nextInt(elemsExecutor.length);
                if (elemsExecutor[r] == count[r]) {
                    continue;
                }
                i++;
                elemsExecutor[r]++;
            }
        }

        global_i = 0;
        for (List<Long> countList : countListArray) {
            for (int i = 0; i < countList.size(); i++) {
                countList.set(i, elemsExecutor[global_i]);
                global_i++;
            }
        }
    }

}
