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
package org.ignis.backend.cluster.helpers.dataframe;

import org.ignis.backend.cluster.IDataFrame;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IDataIOHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataIOHelper.class);

    public IDataIOHelper(IDataFrame data, IProperties properties) {
        super(data, properties);
    }

    public ILazy<Void> saveAsPartitionObjectFile(String path, byte compression) throws IgnisException{
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        LOGGER.info(log() + "Registering saveAsPartitionObjectFile path: " + path + ", compression: " + compression);
        for (IExecutor executors : data.getExecutors()) {
             //builder.newTask(null);//TODO
        }
        return () -> {
            builder.build().start(data.getPool());
            return null;
        };
    }

    public ILazy<Void> saveAsTextFile(String path) throws IgnisException{
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        LOGGER.info(log() + "Registering saveAsTextFile path: " + path);
        for (IExecutor executors : data.getExecutors()) {
             //builder.newTask(null);//TODO
        }
        return () -> {
            builder.build().start(data.getPool());
            return null;
        };
    }

    public ILazy<Void> saveAsJsonFile(String path) throws IgnisException{
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        LOGGER.info(log() + "Registering saveAsJsonFile path: " + path);
        for (IExecutor executors : data.getExecutors()) {
            //builder.newTask(null);//TODO
        }
        return () -> {
            builder.build().start(data.getPool());
            return null;
        };
    }
}
