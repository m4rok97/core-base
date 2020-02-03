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
package org.ignis.backend.cluster;

import java.util.List;
import org.ignis.backend.cluster.helpers.dataframe.IDataCacheHelper;
import org.ignis.backend.cluster.tasks.ICache;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.IThreadPool;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;

/**
 *
 * @author César Pomar
 */
public final class IDataFrame {

    private String name;
    private final long id;
    private final ICache cache;
    private final IWorker worker;
    private final List<IExecutor> executors;
    private final ITaskGroup tasks;

    public IDataFrame(String name, long id, IWorker worker, List<IExecutor> executors, ITaskGroup tasks) throws IgnisException {
        this.id = id;
        this.worker = worker;
        this.executors = executors;
        this.cache = new ICache(id);
        setName(name);
        this.tasks = new IDataCacheHelper(this, worker.getProperties()).create(tasks, cache);
    }

    public List<IExecutor> getExecutors() {
        return executors;
    }

    public int getPartitions() {
        return executors.size();
    }

    public IProperties getProperties() {
        return worker.getProperties();
    }

    public ICache getCache() {
        return cache;
    }

    public ITaskGroup getTasks() {
        return tasks;
    }

    public IWorker getWorker() {
        return worker;
    }

    public ILock getLock() {
        return worker.getLock();
    }

    public IThreadPool getPool() {
        return worker.getThreadPool();
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (name.isEmpty()) {
            name = worker.getName() + ", DataFrame(" + getId() + ")";
        }
        this.name = name;
    }

    public IDataFrame createDataFrame(String name, ITaskGroup tasks) throws IgnisException{
        return worker.createDataFrame(name, executors, tasks);
    }

}
