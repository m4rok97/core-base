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
import org.ignis.backend.cluster.helpers.dataframe.IDataHelper;
import org.ignis.backend.cluster.tasks.ICache;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.ITaskGroupCache;
import org.ignis.backend.cluster.tasks.executor.IUncacheTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.driver.IDataFrameId;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class IDataCacheHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataCacheHelper.class);

    public IDataCacheHelper(IDataFrame data, IProperties properties) {
        super(data, properties);
    }

    public ITaskGroup create(ITaskGroup group, ICache cache) throws IgnisException{
        ITaskGroupCache.Builder builder = new ITaskGroupCache.Builder(data.getLock(), cache, group);
        for (IExecutor executor : data.getExecutors()) {
            builder.addExecutor(getName(), executor);
        }
        return builder.build();
    }

    public void persist(byte level) throws IgnisException{//TODO change level cache when isCached
        data.getCache().setLevel(level);
    }

    public void cache() throws IgnisException{
        data.getCache().setLevel(ICache.PRESERVE);
    }

    public void uncache() throws IgnisException {
        if(!data.getCache().isCached()){
            data.getCache().setLevel(ICache.NO_CACHE);
            return;
        }        
        
        ITaskGroup.Builder builder = new ITaskGroup.Builder();
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IUncacheTask(getName(), executor, data.getCache().getId()));
        }

        builder.build().start(data.getPool());
        data.getCache().setLevel(ICache.NO_CACHE);
        data.getCache().setCached(false);
    }

}
