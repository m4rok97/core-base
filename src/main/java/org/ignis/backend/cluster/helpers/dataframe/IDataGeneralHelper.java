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
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.executor.IApplyPartitionTask;
import org.ignis.backend.cluster.tasks.executor.IFilterTask;
import org.ignis.backend.cluster.tasks.executor.IFlatmapTask;
import org.ignis.backend.cluster.tasks.executor.IMapPartitionsTask;
import org.ignis.backend.cluster.tasks.executor.IMapPartitionsWithIndexTask;
import org.ignis.backend.cluster.tasks.executor.IMapTask;
import org.ignis.backend.cluster.tasks.executor.ISortTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IDataGeneralHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataGeneralHelper.class);

    public IDataGeneralHelper(IDataFrame data, IProperties properties) {
        super(data, properties);
    }

    public IDataFrame map(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMapTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame("", builder.build());
        LOGGER.info(log() + "Registering map -> " + target.getName());
        return target;
    }

    public IDataFrame filter(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IFilterTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame("", builder.build());
        LOGGER.info(log() + "Registering filter -> " + target.getName());
        return target;
    }

    public IDataFrame flatmap(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IFlatmapTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame("", builder.build());
        LOGGER.info(log() + "Registering flatmap -> " + target.getName());
        return target;
    }

    public IDataFrame mapPartitions(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMapPartitionsTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame("", builder.build());
        LOGGER.info(log() + "Registering mapPartitions -> " + target.getName());
        return target;
    }

    public IDataFrame mapPartitionsWithIndex(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMapPartitionsWithIndexTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame("", builder.build());
        LOGGER.info(log() + "Registering mapPartitionsWithIndex -> " + target.getName());
        return target;
    }

    public IDataFrame applyPartition(ISource src) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IApplyPartitionTask(getName(), executor, src));
        }
        IDataFrame target = data.createDataFrame("", builder.build());
        LOGGER.info(log() + "Registering applyPartition -> " + target.getName());
        return target;
    }

    public IDataFrame groupBy(ISource src) throws IgnisException {
        throw new UnsupportedOperationException("Not supported on this version."); //TODO next version
    }

    public IDataFrame groupBy(ISource src, long numPartitions) throws IgnisException {
        throw new UnsupportedOperationException("Not supported on this version."); //TODO next version
    }

    public IDataFrame sort(boolean ascending) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortTask(getName(), executor, ascending));
        }
        IDataFrame target = data.createDataFrame("", builder.build());
        LOGGER.info(log() + "Registering sort ascending: " + ascending + " -> " + target.getName()
        );
        return target;
    }

    public IDataFrame sort(boolean ascending, long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortTask(getName(), executor, ascending, numPartitions));
        }
        IDataFrame target = data.createDataFrame("", builder.build());
        LOGGER.info(log() + "Registering sort ascending: " + ascending + ", numPartitions: " + numPartitions
                + " -> " + target.getName()
        );
        return target;
    }

    public IDataFrame sortBy(ISource src, boolean ascending) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortTask(getName(), executor, src, ascending));
        }
        IDataFrame target = data.createDataFrame("", builder.build());
        LOGGER.info(log() + "Registering sortBy ascending: " + ascending + " -> " + target.getName()
        );
        return target;
    }

    public IDataFrame sortBy(ISource src, boolean ascending, long numPartitions) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getTasks());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISortTask(getName(), executor, src, ascending, numPartitions));
        }
        IDataFrame target = data.createDataFrame("", builder.build());
        LOGGER.info(log() + "Registering sortBy ascending: " + ascending + ", numPartitions: " + numPartitions
                + " -> " + target.getName()
        );
        return target;
    }

}
