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
import org.ignis.backend.cluster.IDriver;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.executor.ICountTask;
import org.ignis.backend.cluster.tasks.executor.IMaxTask;
import org.ignis.backend.cluster.tasks.executor.IMinTask;
import org.ignis.backend.cluster.tasks.executor.ISampleTask;
import org.ignis.backend.cluster.tasks.executor.ITakeSampleTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IDataMathHelper extends IDataHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDataMathHelper.class);

    public IDataMathHelper(IDataFrame data, IProperties properties) {
        super(data, properties);
    }

    public IDataFrame sample(boolean withReplacement, double fraction, int seed) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        ISampleTask.Shared shared = new ISampleTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ISampleTask(getName(), executor, shared, withReplacement, fraction, seed));
        }
        IDataFrame target = data.createDataFrame("", builder.build());
        LOGGER.info(log() + "Registering partitions sample withReplacement: " + withReplacement + ", fraction: "
                + fraction + ", seed:" + seed + " -> " + target.getName());
        return target;
    }

    public ILazy<Long> takeSample(IDriver driver, boolean withReplacement, long num, int seed) {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        LOGGER.info(log() + "Registering takeSample withReplacement: " + withReplacement + ", num: " + num + ", seed: " + seed);
        ITakeSampleTask.Shared shared = new ITakeSampleTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ITakeSampleTask(getName(), executor, shared, false, withReplacement, num, seed));
        }

        builder.newLock(driver.getLock());
        builder.newTask(new ITakeSampleTask(getName(), driver.getExecutor(), shared, true, withReplacement, num, seed));

        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> count() throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        LOGGER.info(log() + "Registering count");
        ICountTask.Shared shared = new ICountTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new ICountTask(getName(), executor, shared));
        }
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> max(IDriver driver, ISource cmp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        IMaxTask.Shared shared = new IMaxTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMaxTask(getName(), executor, shared, false, cmp));
        }

        builder.newLock(driver.getLock());
        builder.newTask(new IMaxTask(driver.getName(), driver.getExecutor(), shared, true, cmp));

        LOGGER.info(log() + "Registering max");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

    public ILazy<Long> min(IDriver driver, ISource cmp) throws IgnisException {
        ITaskGroup.Builder builder = new ITaskGroup.Builder(data.getLock());
        builder.newDependency(data.getWorker().getTasks());
        IMinTask.Shared shared = new IMinTask.Shared(data.getExecutors().size());
        for (IExecutor executor : data.getExecutors()) {
            builder.newTask(new IMinTask(getName(), executor, shared, false, cmp));
        }

        builder.newLock(driver.getLock());
        builder.newTask(new IMinTask(driver.getName(), driver.getExecutor(), shared, true, cmp));

        LOGGER.info(log() + "Registering min");
        return () -> {
            ITaskContext context = builder.build().start(data.getPool());
            return context.<Long>get("result");
        };
    }

}
