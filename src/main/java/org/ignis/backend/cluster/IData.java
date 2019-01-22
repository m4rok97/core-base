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

import java.nio.ByteBuffer;
import java.util.List;
import org.ignis.backend.cluster.helpers.data.IDataCacheHelper;
import org.ignis.backend.cluster.helpers.data.IDataCollectHelper;
import org.ignis.backend.cluster.helpers.data.IDataFilterHelper;
import org.ignis.backend.cluster.helpers.data.IDataFlatmapHelper;
import org.ignis.backend.cluster.helpers.data.IDataKeyByHelper;
import org.ignis.backend.cluster.helpers.data.IDataMapHelper;
import org.ignis.backend.cluster.helpers.data.IDataReduceHelper;
import org.ignis.backend.cluster.helpers.data.IDataSaveHelper;
import org.ignis.backend.cluster.helpers.data.IDataShuffleHelper;
import org.ignis.backend.cluster.helpers.data.IDataSortHelper;
import org.ignis.backend.cluster.helpers.data.IDataTakeHelper;
import org.ignis.backend.cluster.helpers.data.IDataValuesHelper;
import org.ignis.backend.cluster.tasks.ICacheSheduler;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.cluster.tasks.ITaskScheduler;
import org.ignis.backend.cluster.tasks.IThreadPool;
import org.ignis.rpc.ISource;

/**
 *
 * @author César Pomar
 */
public final class IData {

    private final long id;
    private final IJob job;
    private final List<IExecutor> executors;
    private final ICacheSheduler scheduler;
    private String name;

    public IData(long id, IJob job, List<IExecutor> executors, ITaskScheduler scheduler) {
        this.id = id;
        this.job = job;
        this.executors = executors;
        setName("");
        this.scheduler = new IDataCacheHelper(this, job.getProperties()).create(scheduler);//Must be the last
    }

    public List<IExecutor> getExecutors() {
        return executors;
    }

    public int getPartitions() {
        return executors.size();
    }

    public ITaskScheduler getScheduler() {
        return scheduler;
    }

    public ICacheSheduler getCacheScheduler() {
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

    public IData map(ISource function) {
        return new IDataMapHelper(this, job.getProperties()).map(function);
    }

    public IData streamingMap(ISource function, boolean ordered) {
        return new IDataMapHelper(this, job.getProperties()).streamingMap(function, ordered);
    }

    public IData flatmap(ISource function) {
        return new IDataFlatmapHelper(this, job.getProperties()).flatmap(function);
    }

    public IData streamingFlatmap(ISource function, boolean ordered) {
        return new IDataFlatmapHelper(this, job.getProperties()).streamingFlatmap(function, ordered);
    }

    public IData filter(ISource function) {
        return new IDataFilterHelper(this, job.getProperties()).filter(function);
    }

    public IData streamingFilter(ISource function, boolean ordered) {
        return new IDataFilterHelper(this, job.getProperties()).streamingFilter(function, ordered);
    }

    public IData keyBy(ISource function) {
        return new IDataKeyByHelper(this, job.getProperties()).keyBy(function);
    }

    public IData streamingKeyBy(ISource function, boolean ordered) {
        return new IDataKeyByHelper(this, job.getProperties()).streamingKeyBy(function, ordered);
    }

    public IData reduceByKey(ISource function) {
        return new IDataReduceHelper(this, job.getProperties()).reduceByKey(function);
    }

    public IData values() {
        return new IDataValuesHelper(this, job.getProperties()).values();
    }

    public IData shuffle() {
        return new IDataShuffleHelper(this, job.getProperties()).shuffle();
    }

    public ILazy<List<ByteBuffer>> take(long n, boolean light) {
        return new IDataTakeHelper(this, job.getProperties()).take(n, light);
    }

    public ILazy<List<ByteBuffer>> takeSample(long n, boolean withRemplacement, int seed, boolean light) {
        return new IDataTakeHelper(this, job.getProperties()).takeSample(n, withRemplacement, seed, light);
    }

    public ILazy<List<ByteBuffer>> collect(boolean light) {
        return new IDataCollectHelper(this, job.getProperties()).collect(light);
    }

    public IData sort(boolean ascending) {
        return new IDataSortHelper(this, job.getProperties()).sort(ascending);
    }

    public IData sortBy(ISource function, boolean ascending) {
        return new IDataSortHelper(this, job.getProperties()).sortBy(function, ascending);
    }

    public ILazy<Void> saveAsTextFile(String path, boolean join) {
        return new IDataSaveHelper(this, job.getProperties()).saveAsTextFile(path, join);
    }

    public ILazy<Void> saveAsJsonFile(String path, boolean join) {
        return new IDataSaveHelper(this, job.getProperties()).saveAsJsonFile(path, join);
    }

    public void cache() {
        new IDataCacheHelper(this, job.getProperties()).cache();
    }

    public void uncache() {
        new IDataCacheHelper(this, job.getProperties()).uncache();
    }

}
