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

import org.ignis.backend.cluster.helpers.cluster.IClusterCreateHelper;
import org.ignis.backend.cluster.helpers.cluster.IClusterDestroyHelper;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.IThreadPool;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.scheduler.IScheduler;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author César Pomar
 */
public final class ICluster {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ICluster.class);

    private String name;
    private final long id;
    private final ILock lock;
    private final ITaskGroup tasks;
    private final IThreadPool pool;
    private final IProperties properties;
    private final List<IContainer> containers;
    private final List<IWorker> workers;

    public ICluster(String name, long id, IProperties properties, IThreadPool pool, IScheduler scheduler, ISSH ssh)
            throws IgnisException {
        this.id = id;
        this.properties = properties;
        this.pool = pool;
        this.containers = new ArrayList<>();
        this.workers = new ArrayList<>();
        this.lock = new ILock(id);
        setName(name);
        this.tasks = new IClusterCreateHelper(this, properties).create(scheduler, ssh);//Must be the last
        if (Boolean.getBoolean(IKeys.DEBUG)) {
            LOGGER.info("Debug: " + getName() + " " + properties.toString());
        }
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

    public boolean isRunning() {
        return !containers.isEmpty() && containers.get(0).getInfo() != null;
    }

    public void setName(String name) {
        if (name.isEmpty()) {
            name = "Cluster(" + id + ")";
        }
        this.name = name;
    }

    public ITaskGroup getTasks() {
        return tasks;
    }

    public IProperties getProperties() {
        return properties;
    }

    public List<IContainer> getContainers() {
        return containers;
    }

    public IWorker createWorker(String name, String type, int cores, int instances) throws IgnisException {
        IWorker worker = new IWorker(name, workers.size(), this, type, cores, instances);
        workers.add(worker);
        return worker;
    }

    public IWorker getWorker(long id) throws IgnisException {
        synchronized (lock) {
            if (workers.size() > id) {
                return workers.get((int) id);
            }
        }
        throw new IgnisException("Worker doesn't exist");
    }

    public int workers() {
        return workers.size();
    }

    public ILazy<Void> start() {
        return () -> {
            tasks.start(pool);
            return null;
        };
    }

    public ILazy<Void> destroy(IScheduler scheduler) {
        return new IClusterDestroyHelper(this, properties).destroy(scheduler);
    }

}
