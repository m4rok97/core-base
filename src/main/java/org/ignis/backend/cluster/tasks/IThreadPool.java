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

import org.ignis.backend.cluster.ITaskContext;

import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author César Pomar
 */
public final class IThreadPool {

    private final ThreadPoolExecutor pool;
    private final int retries;

    public IThreadPool(int threads, int retries) {
        pool = new ThreadPoolExecutor(threads, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue<>());
        this.retries = retries;
    }

    int getRetries() {
        return retries;
    }

    Future<ITaskGroup> submit(ITaskGroup scheduler, ITaskContext context) {
        return pool.submit(() -> {
            scheduler.start(this, context, retries);
            return scheduler;
        });
    }

    Future<ITask> submit(ITask task, ITaskContext context) {
        return pool.submit(() -> {
            task.start(context);
            return task;
        });
    }

    public void destroy() {
        pool.shutdownNow();
    }

}
