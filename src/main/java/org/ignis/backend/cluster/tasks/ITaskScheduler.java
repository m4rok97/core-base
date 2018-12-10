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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.ignis.backend.cluster.IExecutionContext;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class ITaskScheduler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ITaskScheduler.class);

    public static class Builder {

        private final List<ITask> tasks;
        private final List<ITaskScheduler> depencencies;
        private final List<ILock> locks;

        public Builder(ILock lock) {
            this.tasks = new ArrayList<>();
            this.depencencies = new ArrayList<>();
            this.locks = new ArrayList<>();
            this.locks.add(lock);
        }

        public Builder newTask(ITask task) {
            tasks.add(task);
            return this;
        }

        public Builder newDependency(ITaskScheduler scheduler) {
            depencencies.add(scheduler);
            return this;
        }

        public Builder newLock(ILock lock) {
            locks.add(lock);
            return this;
        }

        public ITaskScheduler build() {
            return new ITaskScheduler(tasks, locks, depencencies);
        }

    }

    private final List<ITask> tasks;
    private final List<ITaskScheduler> depencencies;
    private final List<ILock> locks;

    protected ITaskScheduler(List<ITask> tasks, List<ILock> locks, List<ITaskScheduler> depencencies) {
        this.tasks = tasks;
        this.locks = locks;
        this.depencencies = depencencies;
        locks.sort(Comparator.naturalOrder());
    }

    public final IExecutionContext execute(IThreadPool pool) throws IgnisException {
        IExecutionContext context = new IExecutionContext();
        execute(pool, new IExecutionContext());
        return context;
    }

    protected void execute(IThreadPool pool, IExecutionContext context) throws IgnisException {
        for (int _try = 0;; _try++) {
            List<Future<ITaskScheduler>> depFutures = new ArrayList<>();
            for (ITaskScheduler dependency : depencencies) {
                depFutures.add(pool.submit(dependency, context));
            }
            IgnisException error = null;
            for (int i = 0; i < depFutures.size(); i++) {
                try {
                    depFutures.get(i).get();
                } catch (InterruptedException | ExecutionException ex) {
                    if (ex.getCause() instanceof IgnisException) {
                        error = (IgnisException) ex.getCause();
                    } else {
                        error = new IgnisException(ex.getMessage(), ex);
                    }
                }
            }
            if (error != null) {
                throw error;
            }
            if (tasks.isEmpty()) {
                return;
            }
            try {
                execute(pool, locks, context);
            } catch (IgnisException ex) {
                if (_try == pool.getMaxFailures()) {
                    throw ex;
                }
                LOGGER.error("Failed execution attempt " + (_try + 1) + ", retrying", ex);
                continue;
            }
            break;
        }
    }

    private void execute(IThreadPool pool, List<ILock> locks, IExecutionContext context) throws IgnisException {
        if (locks.isEmpty()) {
            List<Future<ITask>> futures = new ArrayList<>(tasks.size());
            for (ITask task : tasks) {
                futures.add(pool.submit(task, context));
            }
            IgnisException error = null;
            for (int i = 0; i < futures.size(); i++) {
                try {
                    futures.get(i).get();
                } catch (InterruptedException | ExecutionException ex) {
                    error = new IgnisException("Execution failed", ex);
                    LOGGER.warn(error.getMessage(), ex);
                }
            }
            if (error != null) {
                throw error;
            }
        } else {
            synchronized (locks.get(0)) {
                execute(pool, locks.subList(1, locks.size()), context);
            }
        }
    }

}
