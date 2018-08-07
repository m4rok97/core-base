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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class TaskScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskScheduler.class);

    private final List<Task> tasks;
    private final ILock lock;

    public TaskScheduler(Task... tasks) {
        this(Arrays.asList(tasks));
    }

    public TaskScheduler(List<Task> tasks) {
        this.tasks = tasks;
        this.lock = tasks.isEmpty() ? null : tasks.get(0).lock;
    }

    public void execute(IThreadPool pool) throws IgnisException {
        for (int _try = 0;; _try++) {
            if (tasks.isEmpty()) {
                return;
            }
            for (Task t : tasks) {
                if (lock != t.lock) {
                    throw new IgnisException("Internal Error");
                }
            }
            List<Future<TaskScheduler>> depFutures = new ArrayList<>();
            for (int i = 0; true; i++) {
                List<Task> depTasks = new ArrayList<>();
                for (int j = 0; j < tasks.size(); j++) {
                    if (tasks.get(j).dependencies.length > i) {
                        depTasks.add(tasks.get(j).dependencies[i]);
                    }
                }
                if (depTasks.isEmpty()) {
                    break;
                } else {
                    depFutures.add(pool.submit(new TaskScheduler(depTasks)));
                }
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
            try {
                synchronized (lock) {
                    for (Task task : tasks) {
                        pool.submit(task).get();
                    }
                }
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

}
