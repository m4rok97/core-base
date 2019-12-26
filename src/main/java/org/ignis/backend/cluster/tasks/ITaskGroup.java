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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class ITaskGroup {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ITaskGroup.class);

    public static class Builder {

        protected final List<ITask> tasks;
        protected final List<ITaskGroup> depencencies;
        protected final Set<ILock> locks;

        public Builder() {
            this.tasks = new ArrayList<>();
            this.depencencies = new ArrayList<>();
            this.locks = new HashSet<>();
        }

        public Builder(ILock lock) {
            this();
            newLock(lock);
        }

        public Builder newTask(ITask task) {
            tasks.add(task);
            return this;
        }

        public Builder newDependency(ITaskGroup scheduler) {
            depencencies.add(scheduler);
            return this;
        }

        public Builder newLock(ILock lock) {
            locks.add(lock);
            return this;
        }

        public ITaskGroup build() {
            return new ITaskGroup(tasks, locks, depencencies);
        }

    }

    private final List<ITask> tasks;
    private final List<ITaskGroup> depencencies;
    private final List<ITaskGroup> subTasksGroup;
    private final List<ILock> locks;

    protected ITaskGroup(List<ITask> tasks, Set<ILock> locks, List<ITaskGroup> depencencies) {
        this.tasks = tasks;
        this.locks = new ArrayList<>(locks);
        this.depencencies = depencencies;
        this.subTasksGroup = new ArrayList<>();
        this.locks.sort(Comparator.naturalOrder());
    }

    public List<ITaskGroup> getSubTasksGroup() {
        return subTasksGroup;
    }

    public final ITaskContext start(IThreadPool pool) throws IgnisException {
        ITaskContext context = new ITaskContext();
        start(pool, context, pool.getRetries());
        return context;
    }

    protected void start(IThreadPool pool, ITaskContext context, int retries) throws IgnisException {
        for (int attempt = 0;; attempt++) {
            List<Future<ITaskGroup>> depFutures = new ArrayList<>();
            for (ITaskGroup dependency : depencencies) {
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
                start(pool, locks, context);
            } catch (IgnisException ex) {
                if (attempt == retries) {
                    throw ex;
                }
                LOGGER.error("Failed execution attempt " + (attempt + 1) + ", retrying", ex);
                continue;
            }
            break;
        }
    }

    private void start(IThreadPool pool, List<ILock> locks, ITaskContext context) throws IgnisException {
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
                    if (ex.getCause() instanceof IgnisException) {
                        error = (IgnisException) ex.getCause();
                    } else {
                        error = new IgnisException(ex.getMessage(), ex);
                    }
                    LOGGER.warn(error.getMessage(), ex);
                }
            }
            if (error != null) {
                throw error;
            }
            for (ITaskGroup subtask : subTasksGroup) {
                subtask.start(pool, context, 0);
            }
        } else {
            synchronized (locks.get(0)) {
                start(pool, locks.subList(1, locks.size()), context);
            }
        }
    }

}
