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

import org.hipparchus.distribution.IntegerDistribution;
import org.hipparchus.distribution.continuous.NormalDistribution;
import org.hipparchus.distribution.discrete.BinomialDistribution;
import org.hipparchus.distribution.discrete.HypergeometricDistribution;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;

/**
 * @author César Pomar
 */
public class ISampleTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ISampleTask.class);

    public static class Shared {

        @SuppressWarnings("unchecked")
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
    public void contextError(IgnisException ex) throws IgnisException {
        shared.barrier.fails();
        throw ex;
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
                long snum = (long) Math.ceil(elems * fraction);
                if (!withReplacement && elems < snum) {
                    throw new IgnisException("There are not enough elements");
                }
                sample(shared.count, snum, withReplacement, seed);
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

    static void sample(List<Long>[] flatCounts, long sample, boolean withReplacement, int seed) {
        if (flatCounts.length == 1 && flatCounts[0].size() == 1) {
            flatCounts[0].set(0, sample);
            return;
        }
        if (sample == 0) {
            for (List<Long> flatCount : flatCounts) {
                Collections.fill(flatCount, 0L);
            }
            return;
        }
        int parts = Arrays.stream(flatCounts).map(List::size).reduce(0, Integer::sum);
        long[] count = new long[parts];
        for (int n = 0, i = 0; i < flatCounts.length; i++) {
            for (int j = 0; j < flatCounts[i].size(); j++) {
                count[n++] = flatCounts[i].get(j);
            }
        }

        List<long[]> levels = new ArrayList<>();
        levels.add(count);

        long[] top = count;
        while (top.length > 1) {
            long[] leveln = new long[(int) Math.ceil(top.length / 2.0)];
            for (int i = 0; i < leveln.length; i++) {
                if (2 * i + 1 == top.length) {
                    leveln[i] = top[2 * i];
                    continue;
                }
                leveln[i] = top[2 * i] + top[2 * i + 1];
            }
            levels.add(leveln);
            top = leveln;
        }

        Random r = new Random(seed);
        sampleTraverse(0, levels.size() - 1, levels, sample, r, withReplacement);

        for (int n = 0, i = 0; i < flatCounts.length; i++) {
            for (int j = 0; j < flatCounts[i].size(); j++) {
                flatCounts[i].set(j, count[n++]);
            }
        }
    }

    private static void sampleTraverse(int i, int l, List<long[]> levels, long sample, Random r, boolean withReplacement) {
        if (l == 0) {
            levels.get(0)[i] = sample;
            return;
        }
        long total = levels.get(l)[i];
        long[] childs = levels.get(l - 1);

        if (childs.length > 2 * i + 1) {
            long k = sampleSize(childs[2 * i], sample, total, r, withReplacement);
            sampleTraverse(2 * i, l - 1, levels, k, r, withReplacement);
            sampleTraverse(2 * i + 1, l - 1, levels, sample - k, r, withReplacement);
        } else {
            sampleTraverse(2 * i, l - 1, levels, sample, r, withReplacement);
        }
    }

    private static long sampleSize(long elems, long sample, long total, Random r, boolean withReplacement) {
        IntegerDistribution dist;
        if (total > Integer.MAX_VALUE) {
            if (sample == 0) {
                return 0;
            }
            double mean, sd;
            if (withReplacement) {
                final double p = ((double) elems / total);
                mean = sample * p;
                sd = mean * (1 - p);
            } else {
                final double N = total;
                final double m = elems;
                final double n = sample;
                mean = n * m / N;
                sd = (n * m * (N - n) * (N - m)) / (N * N * (N - 1));
            }
            sd = Math.max(sd, 10e-6);
            NormalDistribution dist2 = new NormalDistribution(mean, sd);
            double k = dist2.inverseCumulativeProbability(r.nextDouble());
            if (k < 0) {
                k = 0;
            }
            if (!withReplacement && sample > k + total - elems) {
                k = sample + elems - total;
            }
            return Math.min((long) k, withReplacement ? sample : elems);
        }
        if (withReplacement) {
            dist = new BinomialDistribution((int) sample, (double) elems / total);
        } else {
            dist = new HypergeometricDistribution((int) total, (int) elems, (int) sample);
        }
        return dist.inverseCumulativeProbability(r.nextDouble());
    }

}
