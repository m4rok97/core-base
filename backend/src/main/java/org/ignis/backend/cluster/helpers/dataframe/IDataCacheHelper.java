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
package org.ignis.backend.cluster.helpers.dataframe;

import org.ignis.backend.cluster.IDataFrame;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.tasks.ICache;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.ITaskGroupCache;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public class IDataCacheHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataCacheHelper.class);

    public IDataCacheHelper(IDataFrame data, IProperties properties) {
        super(data, properties);
    }

    public ITaskGroup create(ITaskGroup group, ICache cache) throws IgnisException {
        ITaskGroupCache.Builder builder = new ITaskGroupCache.Builder(data.getLock(), cache, group);
        builder.newDependency(data.getWorker().getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.addExecutor(getName(), executor);
        }
        return builder.build();
    }

    public void persist(byte level) throws IgnisException {
        data.getCache().setNextLevel(ICache.Level.fromInt(level));
    }

    public void cache() throws IgnisException {
        data.getCache().setNextLevel(ICache.Level.PRESERVE);
    }

    public ILazy<Void> uncache() throws IgnisException {
        data.getCache().setNextLevel(ICache.Level.NO_CACHE);
        if (!data.getCache().isCached()) {
            return () -> {
                return null;
            };
        }
        return () -> {
            data.getTasks().start(data.getPool());
            return null;
        };
    }

}
