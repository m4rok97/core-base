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
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;

import java.util.concurrent.*;

/**
 * @author César Pomar
 */
public final class IThreadPool {

    private final ExecutorService pool;
    private final int attempts;

    public IThreadPool(IProperties props) {
        this.pool = Executors.newVirtualThreadPerTaskExecutor();
        this.attempts = props.getInteger(IKeys.MODULES_RECOVERY_ATTEMPS);
    }

    int getAttempts() {
        return attempts;
    }

    Future<ITaskGroup> submit(ITaskGroup scheduler, ITaskContext context) {
        return pool.submit(() -> {
            scheduler.start(this, context, attempts);
            return scheduler;
        });
    }

    Future<ITask> submit(ITask task, ITaskContext context, IWaitObject waitObject) {
        return pool.submit(() -> {
            try{
                task.start(context);
                return task;
            }catch (Exception ex){
                throw ex;
            }finally {
                waitObject.addFinishedTask();
            }
        });
    }

    public void destroy() {
        pool.shutdownNow();
    }

}
