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
import org.ignis.backend.cluster.helpers.cluster.IClusterCreateHelper;
import org.ignis.backend.cluster.helpers.cluster.IClusterFileHelper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.cluster.tasks.ILock;

/**
 *
 * @author César Pomar
 */
public class ICluster {

    private final long id;
    private final IProperties properties;
    private final List<IContainer> containers;
    private final List<IJob> jobs;
    private final ILock lock;
    private boolean keep;

    public ICluster(long id, IProperties properties) throws IgnisException {
        this.id = id;
        this.properties = properties;
        this.jobs = new ArrayList<>();
        this.lock = new ILock(id);
        this.containers = new IClusterCreateHelper(this, properties).create(lock);
    }

    public long getId() {
        return id;
    }

    public ILock getLock() {
        return lock;
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

    public int sendFiles(String source, String target) {
        return new IClusterFileHelper(this, properties).sendFiles(source, target);
    }

    public int sendCompressedFile(String source, String target) {
        return new IClusterFileHelper(this, properties).sendCompressedFile(source, target);
    }

    public boolean isKeep() {
        return keep;
    }

    public void setKeep(boolean keep) {
        this.keep = keep;
    }

}
