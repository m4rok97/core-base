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
import org.ignis.backend.cluster.helpers.data.IDataMapHelper;
import org.ignis.backend.cluster.helpers.data.IDataReduceHelper;
import org.ignis.backend.cluster.helpers.data.IDataSaveHelper;
import org.ignis.backend.cluster.helpers.data.IDataShuffleHelper;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.cluster.tasks.IThreadPool;
import org.ignis.backend.cluster.tasks.TaskScheduler;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.ISourceFunction;

/**
 *
 * @author César Pomar
 */
public final class IData {

    private final long id;
    private final IJob job;
    private final List<IExecutor> executors;
    private final TaskScheduler scheduler;
    private String name;

    public IData(long id, IJob job, List<IExecutor> executors, TaskScheduler scheduler) {
        this.id = id;
        this.job = job;
        this.executors = executors;
        this.scheduler = scheduler;
        setName("");
    }

    public List<IExecutor> getExecutors() {
        return executors;
    }

    public int getPartitions() {
        return executors.size();
    }

    public TaskScheduler getScheduler() {
        return scheduler;
    }

    public IJob getJob() {
        return job;
    }

    public ILock getLock() {
        return job.getLock();
    }

    public IThreadPool getPool() {
        return job.getPool();
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (name.isEmpty()) {
            name = job.getName() + ", Data(" + getId() + ")";
        }
        this.name = name;
    }

    public void setKeep(int level) {
        //TODO
    }

    public IData map(ISourceFunction function) {
        return new IDataMapHelper(this, job.getProperties()).map(function);
    }

    public IData streamingMap(ISourceFunction function, boolean ordered) {
        return new IDataMapHelper(this, job.getProperties()).streamingMap(function, ordered);
    }

    public IData reduceByKey(ISourceFunction function) {
        return new IDataReduceHelper(this, job.getProperties()).reduceByKey(function);
    }

    public IData shuffle() {
        return new IDataShuffleHelper(this, job.getProperties()).shuffle();
    }

    public void saveAsTextFile(String path, boolean join) throws IgnisException {
        new IDataSaveHelper(this, job.getProperties()).saveAsTextFile(path, join);
    }

    public void saveAsJsonFile(String path, boolean join) throws IgnisException {
        new IDataSaveHelper(this, job.getProperties()).saveAsJsonFile(path, join);
    }

}
