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
package org.ignis.backend.cluster.tasks.executor;

import org.apache.thrift.TException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IExecutorExceptionWrapper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.IExecutorException;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class ISortByKeyTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ISortByKeyTask.class);

    private final boolean ascending;
    private final ISource src;
    private final Long partitions;

    public ISortByKeyTask(String name, IExecutor executor, ISource src, boolean ascending, Long partitions) {
        super(name, executor, Mode.LOAD_AND_SAVE);
        this.src = src;
        this.ascending = ascending;
        this.partitions = partitions;
    }

    public ISortByKeyTask(String name, IExecutor executor, ISource src, boolean ascending) {
        this(name, executor, src, ascending, null);
    }

    public ISortByKeyTask(String name, IExecutor executor, boolean ascending) {
        this(name, executor, null, ascending, null);
    }

    public ISortByKeyTask(String name, IExecutor executor, boolean ascending, Long partitions) {
        this(name, executor, null, ascending, partitions);
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        try {
            LOGGER.info(log() + "sortByKey started");

            if (src == null) {
                if (partitions == null) {
                    executor.getGeneralModule().sortByKey(ascending);
                } else {
                    executor.getGeneralModule().sortByKey2a(ascending, partitions);
                }
            } else {
                if (partitions == null) {
                    executor.getGeneralModule().sortByKey2b(src, ascending);
                } else {
                    executor.getGeneralModule().sortByKey3(src, ascending, partitions);
                }
            }
            LOGGER.info(log() + "sortByKey finished");
        } catch (IExecutorException ex) {
            throw new IExecutorExceptionWrapper(ex);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

}
