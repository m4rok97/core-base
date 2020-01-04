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

import java.util.ArrayList;
import java.util.List;
import org.ignis.backend.cluster.helpers.worker.IWorkerCreateHelper;
import org.ignis.backend.cluster.helpers.worker.IWorkerDestroyHelper;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.IThreadPool;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.properties.IProperties;

/**
 *
 * @author César Pomar
 */
public final class IWorker {

    private String name;
    private final long id;
    private final String type;
    private final int cores;
    private final ICluster cluster;
    private final ILock lock;
    private final ITaskGroup tasks;
    private final List<IExecutor> executors;
    private final List<IDataFrame> dataFrames;

    public IWorker(String name, long id, ICluster cluster, String type, int cores) throws IgnisException {
        this.id = id;
        this.cluster = cluster;
        this.type = type;
        this.cores = cores;
        this.dataFrames = new ArrayList<>();
        this.executors = new ArrayList<>();
        setName(name);
        if (cluster.getProperties().getBoolean(IKeys.EXECUTOR_ISOLATION)) {
            this.lock = cluster.getLock();
        } else {
            this.lock = new ILock(id, true);
        }
        this.tasks = new IWorkerCreateHelper(this, cluster.getProperties()).create();//Must be the last
    }

    public long getId() {
        return id;
    }

    public ILock getLock() {
        return lock;
    }

    public IThreadPool getThreadPool() {
        return cluster.getPool();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (name.isEmpty()) {
            name = cluster.getName() + ", Worker(" + id + ")";
        }
        this.name = name;
    }

    public List<IExecutor> getExecutors() {
        return executors;
    }

    public ICluster getCluster() {
        return cluster;
    }

    public String getType() {
        return type;
    }

    public IProperties getProperties() {
        return cluster.getProperties();
    }

    public int getCores() {
        return cores;
    }

    public ITaskGroup getTasks() {
        return tasks;
    }

    public IDataFrame createDataFrame(String name, List<IExecutor> executors, ITaskGroup tasks) throws IgnisException {
        IDataFrame data = new IDataFrame(name, dataFrames.size(), this, executors, tasks);
        dataFrames.add(data);
        return data;
    }

    public IDataFrame getDataFrame(long id) throws IgnisException {
        synchronized (lock) {
            if (dataFrames.size() > id) {
                return dataFrames.get((int) id);
            }
        }
        throw new IgnisException("IDataFrame doesn't exist");
    }

    public void destroy() throws IgnisException {
        new IWorkerDestroyHelper(this, cluster.getProperties()).destroy();
    }

}
