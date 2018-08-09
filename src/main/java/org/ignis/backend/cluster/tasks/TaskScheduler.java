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
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class TaskScheduler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TaskScheduler.class);

    public static class Builder {

        private final List<Task> tasks;
        private final List<TaskScheduler> depencencies;
        private final List<ILock> locks;

        public Builder(ILock lock) {
            this.tasks = new ArrayList<>();
            this.depencencies = new ArrayList<>();
            this.locks = new ArrayList<>();
            this.locks.add(lock);
        }

        public Builder newTask(Task task) {
            tasks.add(task);
            return this;
        }

        public Builder newDependency(TaskScheduler scheduler) {
            depencencies.add(scheduler);
            return this;
        }

        public Builder newLock(ILock lock) {
            locks.add(lock);
            return this;
        }

        public TaskScheduler build() {
            return new TaskScheduler(tasks, locks, depencencies);
        }

    }

    private final List<Task> tasks;
    private final List<TaskScheduler> depencencies;
    private final List<ILock> locks;

    private TaskScheduler(List<Task> tasks, List<ILock> locks, List<TaskScheduler> depencencies) {
        this.tasks = tasks;
        this.locks = locks;
        this.depencencies = depencencies;
        locks.sort(Comparator.naturalOrder());
    }

    public void execute(IThreadPool pool) throws IgnisException {
        for (int _try = 0;; _try++) {
            List<Future<TaskScheduler>> depFutures = new ArrayList<>();
            for (TaskScheduler dependency : depencencies) {
                depFutures.add(pool.submit(dependency));
            }
            IgnisException error = null;
            for (int i = 0; i < depFutures.size(); i++) {
                if (error == null) {
                    try {
                        depFutures.get(i).get();
                    } catch (InterruptedException | ExecutionException ex) {
                        error = new IgnisException("Dependency execution failed", ex);
                    }
                } else {
                    depFutures.get(i).cancel(true);
                }

            }
            if (error != null) {
                throw error;
            }
            if (tasks.isEmpty()) {
                return;
            }
            try {
                execute(pool, locks);
            } catch (InterruptedException | ExecutionException ex) {
                if (_try == pool.getShedulerTries() - 1) {
                    throw new IgnisException("Execution failed", ex);
                }
                LOGGER.error("Failed execution attempt " + _try + ", retrying", ex);
                continue;
            }
            break;
        }
    }

    private void execute(IThreadPool pool, List<ILock> locks) throws InterruptedException, ExecutionException {
        if (locks.isEmpty()) {
            List<Future<Task>> futures = new ArrayList<>(tasks.size());
            for (Task task : tasks) {
                futures.add(pool.submit(task));
            }
            for (int i = 0; i < futures.size(); i++) {
                try {
                    futures.get(i).get();
                } catch (InterruptedException | ExecutionException ex) {
                    for (int j = i + 1; j < futures.size(); j++) {
                        try {
                            futures.get(i).get();
                        } catch (InterruptedException | ExecutionException ex2) {
                            LOGGER.warn("Executor Fails", ex2);
                        }
                    }
                    throw ex;
                }
            }
        } else {
            synchronized (locks.get(0)) {
                execute(pool, locks.subList(1, locks.size()));
            }
        }
    }

}
