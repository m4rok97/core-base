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
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.rpc.ISourceFunction;

/**
 *
 * @author César Pomar
 */
public class IData {

    private final long id;
    private final IJob job;
    private final List<ISplit> splits;

    public IData(long id, IJob job, List<ISplit> splits) {
        this.id = id;
        this.job = job;
        this.splits = splits;
    }

    public List<ISplit> getSplits() {
        return splits;
    }

    public int getSplitSize() {
        return splits.size();
    }

    public IJob getJob() {
        return job;
    }

    public ILock getLock() {
        return job.getLock();
    }

    public long getId() {
        return id;
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

    public void saveAsTextFile(String path, boolean join) {
        new IDataSaveHelper(this, job.getProperties()).saveAsTextFile(path, join);
    }

    public void saveAsJsonFile(String path, boolean join) {
         new IDataSaveHelper(this, job.getProperties()).saveAsJsonFile(path, join);
    }

}
