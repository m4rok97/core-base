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

import org.ignis.backend.cluster.IData;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.tasks.Lazy;
import org.ignis.backend.cluster.tasks.TaskScheduler;
import org.ignis.backend.cluster.tasks.executor.ISaveAsJsonFileTask;
import org.ignis.backend.cluster.tasks.executor.ISaveAsTextFileTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IDataSaveHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataSaveHelper.class);

    public IDataSaveHelper(IData data, IProperties properties) {
        super(data, properties);
    }

    public Lazy<Void> saveAsTextFile(String path, boolean joined) throws IgnisException {
        LOGGER.info(log() + "SaveAsTextFile path: " + path + ", joined: " + joined);
        TaskScheduler.Builder shedulerBuilder = new TaskScheduler.Builder(data.getLock());
        shedulerBuilder.newDependency(data.getScheduler());
        int size = data.getExecutors().size();
        for (int i = 0; i < size; i++) {
            String ePath = joined ? path : path + "_part" + i;
            shedulerBuilder.newTask(new ISaveAsTextFileTask(this, data.getExecutors().get(i), ePath, !joined, i != size - 1));
        }
        return () -> {
            shedulerBuilder.build().execute(data.getPool());
            LOGGER.info(log() + "saveAsTextFile Done");
            return null;
        };
    }

    public Lazy<Void> saveAsJsonFile(String path, boolean joined) throws IgnisException {
        LOGGER.info(log() + "SaveAsJsonFile path: " + path + ", joined: " + joined);
        TaskScheduler.Builder shedulerBuilder = new TaskScheduler.Builder(data.getLock());
        shedulerBuilder.newDependency(data.getScheduler());
        int size = data.getExecutors().size();
        for (int i = 0; i < size; i++) {
            String ePath = joined ? path : path + "_part" + i;
            shedulerBuilder.newTask(new ISaveAsJsonFileTask(this, data.getExecutors().get(i), ePath, i == 0, i == size - 1));
        }
        return () -> {
            shedulerBuilder.build().execute(data.getPool());
            LOGGER.info(log() + "saveAsJsonFile Done");
            return null;
        };
    }

}
