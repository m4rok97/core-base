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
import org.ignis.backend.cluster.helpers.job.IJobCreateHelper;
import org.ignis.backend.cluster.helpers.job.IJobImportDataHelper;
import org.ignis.backend.cluster.helpers.job.IJobReadFileHelper;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;

/**
 *
 * @author César Pomar
 */
public class IJob {

    private final long id;
    private final ICluster cluster;
    private final IProperties properties;
    private final List<IExecutor> executors;
    private final List<IData> datas;
    private boolean keep;

    public IJob(long id, ICluster cluster, IProperties properties) throws IgnisException {
        this.id = id;
        this.cluster = cluster;
        this.properties = properties;
        this.datas = new ArrayList<>();
        this.executors = new IJobCreateHelper(this, properties).create();
    }

    public long getId() {
        return id;
    }

    public ILock getLock() {
        return cluster.getLock();
    }

    public List<IExecutor> getExecutors() {
        return executors;
    }

    public ICluster getCluster() {
        return cluster;
    }

    public IProperties getProperties() {
        return properties;
    }

    public boolean isKeep() {
        return keep;
    }

    public void setKeep(boolean keep) {
        this.keep = keep;
    }

    public int getDataSize() {
        return datas.size();
    }

    public IData getData(long id) throws IgnisException {
        IData data = datas.get((int) id);
        if (data == null) {
            throw new IgnisException("Data doesn't exist");
        }
        return data;
    }

    public IData readFile(String path) throws IgnisException {
        return new IJobReadFileHelper(this, properties).readFile(path);
    }

    public IData importData(IData source) throws IgnisException {
        return new IJobImportDataHelper(this, properties).importData(source);
    }

}
