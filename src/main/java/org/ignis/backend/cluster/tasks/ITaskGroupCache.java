/*
 * Copyright (C) 2019 César Pomar
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
package org.ignis.backend.cluster.tasks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.executor.ICacheTask;
import org.ignis.backend.cluster.tasks.executor.ILoadCacheTask;
import org.ignis.backend.exception.IgnisCacheException;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class ITaskGroupCache extends ITaskGroup {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ITaskGroupCache.class);

    public static class Builder {

        private final ICache cache;
        private final ILock lock;
        private final ITaskGroup dependency;
        private final List<ITask> loadTasks;
        private final List<ITask> saveTasks;

        public Builder(ILock lock, ICache cache, ITaskGroup dependency) {
            this.cache = cache;
            this.lock = lock;
            this.dependency = dependency;
            loadTasks = new ArrayList<>();
            saveTasks = new ArrayList<>();
        }

        public Builder addExecutor(String name, IExecutor e) {
            loadTasks.add(new ILoadCacheTask(name, e, cache.getId()));
            saveTasks.add(new ICacheTask(name, e, cache));
            return this;
        }

        public ITaskGroup build() {
            return new ITaskGroupCache(Collections.singleton(lock), Arrays.asList(dependency), cache, loadTasks, saveTasks);
        }

    }

    private final ICache cache;
    private final ITaskGroup loadCache;
    private final ITaskGroup noCache;
    private final ITaskGroup updateCache;

    private ITaskGroupCache(Set<ILock> locks, List<ITaskGroup> depencencies, ICache cache,
            List<ITask> loadTasks, List<ITask> saveTasks) {
        super(saveTasks, locks, depencencies);
        loadCache = new ITaskGroup(loadTasks, locks, new ArrayList<>());
        updateCache = new ITaskGroup(saveTasks, locks, new ArrayList<>());
        noCache = depencencies.get(0);
        this.cache = cache;
    }

    @Override
    protected void start(IThreadPool pool, ITaskContext context, int retries) throws IgnisException {
        if (cache.isCached()) { 
            if (cache.getActualLevel() != cache.getNextLevel()) {//Cache update or destruction
                updateCache.start(pool, context, retries);
                cache.setActualLevel(cache.getNextLevel());
            }
            if (cache.isCached()) { //No run in cache destruction
                try {
                    loadCache.start(pool, context, retries);
                    return;
                } catch (IgnisCacheException ex) {//Cache not found
                    LOGGER.warn("Cache error, loading dependencies", ex);
                    cache.setActualLevel(ICache.Level.NO_CACHE);
                }
            }else{
                return; 
            }
        } 
        
        if (cache.getActualLevel() != cache.getNextLevel()) {//Create cache
            super.start(pool, context, retries);
            cache.setActualLevel(cache.getNextLevel());
        }else{
            noCache.start(pool, context, retries);
        }
    }
}
