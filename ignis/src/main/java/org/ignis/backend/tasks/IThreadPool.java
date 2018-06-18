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
package org.ignis.backend.tasks;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author César Pomar
 */
public final class IThreadPool {

    private final ThreadPoolExecutor pool;
    private final int shedulerTries;

    public IThreadPool(int threads, int shedulerTries) {
        pool = new ThreadPoolExecutor(1, Integer.MAX_VALUE, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        atLeastSize(threads);
        this.shedulerTries = shedulerTries;
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

    public synchronized void atLeastSize(int threads) {
        if (pool.getCorePoolSize() < threads + 1) {
            pool.setCorePoolSize(threads + 1);
        }
    }

    int getShedulerTries() {
        return shedulerTries;
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

}
