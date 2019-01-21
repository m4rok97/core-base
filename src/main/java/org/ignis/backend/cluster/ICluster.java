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
import org.ignis.backend.allocator.IAllocator;
import org.ignis.backend.cluster.helpers.cluster.IClusterCreateHelper;
import org.ignis.backend.cluster.helpers.cluster.IClusterDestroyHelper;
import org.ignis.backend.cluster.helpers.cluster.IClusterFileHelper;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.cluster.tasks.ITaskScheduler;
import org.ignis.backend.cluster.tasks.IThreadPool;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;

/**
 *
 * @author César Pomar
 */
public final class ICluster {

    private final long id;
    private final IThreadPool pool;
    private final IProperties properties;
    private final List<IContainer> containers;
    private final List<IJob> jobs;
    private final ILock lock;
    private final List<ITaskScheduler> schedulers;
    private String name;
    private boolean keep;

    public ICluster(long id, IProperties properties, IThreadPool pool, IAllocator allocator) throws IgnisException {
        this.id = id;
        this.properties = properties;
        this.pool = pool;
        this.jobs = new ArrayList<>();
        this.lock = new ILock(id);
        this.schedulers = new ArrayList<>();
        setName("");
        this.containers = new IClusterCreateHelper(this, properties).create(allocator);//Must be the last
    }

    public long getId() {
        return id;
    }

    public ILock getLock() {
        return lock;
    }

    public IThreadPool getPool() {
        return pool;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (name.isEmpty()) {
            name = "Cluster(" + id + ")";
        }
        this.name = name;
    }

    public void putScheduler(ITaskScheduler scheduler) {
        if (scheduler != null) {
            schedulers.add(scheduler);
        }
    }

    public ITaskScheduler getScheduler() {
        return schedulers.get(schedulers.size() - 1);
    }

    public IProperties getProperties() {
        return properties;
    }

    public List<IContainer> getContainers() {
        return containers;
    }

    public IJob createJob(String type, IProperties properties) throws IgnisException {
        IJob job = new IJob(jobs.size(), this, type, properties);
        jobs.add(job);
        return job;
    }

    public IJob createJob(String type) throws IgnisException {
        return createJob(type, properties);
    }

    public IJob getJob(long id) throws IgnisException {
        IJob job = jobs.get((int) id);
        if (job == null) {
            throw new IgnisException("Job doesn't exist");
        }
        return job;
    }

    public int sendFiles(String source, String target) throws IgnisException {
        return new IClusterFileHelper(this, properties).sendFiles(source, target);
    }

    public int sendCompressedFile(String source, String target) throws IgnisException {
        return new IClusterFileHelper(this, properties).sendCompressedFile(source, target);
    }
    
    public void destroy() throws IgnisException{
        new IClusterDestroyHelper(this, properties).destroy();
    }

    public boolean isKeep() {
        return keep;
    }

    public void setKeep(boolean keep) {
        this.keep = keep;
    }

}
