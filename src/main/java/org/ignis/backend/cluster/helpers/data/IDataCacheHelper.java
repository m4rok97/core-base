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
package org.ignis.backend.cluster.helpers.data;

import java.util.ArrayList;
import java.util.List;
import org.ignis.backend.cluster.IData;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.tasks.ICacheSheduler;
import org.ignis.backend.cluster.tasks.ITask;
import org.ignis.backend.cluster.tasks.ITaskScheduler;
import org.ignis.backend.cluster.tasks.executor.ICacheTask;
import org.ignis.backend.cluster.tasks.executor.ILoadCacheTask;
import org.ignis.backend.cluster.tasks.executor.IMapTask;
import org.ignis.backend.cluster.tasks.executor.IUncacheTask;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class IDataCacheHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataCacheHelper.class);

    public IDataCacheHelper(IData data, IProperties properties) {
        super(data, properties);
    }

    public ICacheSheduler create(ITaskScheduler scheduler) {
        List<ITask> loadCache = new ArrayList<>();
        List<ITask> unCache = new ArrayList<>();
        for (IExecutor executor : data.getExecutors()) {
            loadCache.add(new ILoadCacheTask(this, executor, data.getId()));
            unCache.add(new IUncacheTask(this, executor, data.getId()));
        }
        return new ICacheSheduler(data.getLock(), loadCache, unCache, scheduler);
    }

    public void cache() {
        List<ITask> cache = new ArrayList<>();
        for (IExecutor executor : data.getExecutors()) {
            cache.add(new ICacheTask(this, executor, data.getId()));
        }
        data.getCacheScheduler().cache(cache);
    }

    public void uncache() {
        data.getCacheScheduler().uncache();
    }

}
