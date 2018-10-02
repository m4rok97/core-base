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
package org.ignis.backend.cluster.tasks;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author César Pomar
 */
public final class IThreadPool {

    private final ThreadPoolExecutor pool;
    private final int maxFailures;

    public IThreadPool(int threads, int maxFailures) {
        pool = new ThreadPoolExecutor(threads, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue<>(), new DaemonThreadFactory());
        this.maxFailures = maxFailures;
    }

    public synchronized void increase(int threads) {
        int newSize = pool.getPoolSize() + threads;
        if (newSize > 0) {
            pool.setCorePoolSize(newSize);
        } else {
            pool.setCorePoolSize(1);
        }
    }

    public synchronized void decrease(int threads) {
        increase(-threads);
    }

    int getMaxFailures() {
        return maxFailures;
    }

    Future<TaskScheduler> submit(TaskScheduler scheduler) {
        return pool.submit(() -> {
            scheduler.execute(this);
            return scheduler;
        });
    }

    Future<Task> submit(Task task) {
        return pool.submit(() -> {
            task.execute();
            return task;
        });
    }

    public void destroy() {
        pool.shutdownNow();
    }
    
    private class DaemonThreadFactory implements ThreadFactory{

        private final ThreadFactory factory;
        
        public DaemonThreadFactory() {
            factory = Executors.defaultThreadFactory();
        }

     
        @Override
        public Thread newThread(Runnable r) {
            Thread t = factory.newThread(r);
            t.setDaemon(true);
            return t;
        }
        
    }

}
