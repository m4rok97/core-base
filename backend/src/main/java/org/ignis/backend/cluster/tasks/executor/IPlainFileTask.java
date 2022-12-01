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
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IPlainFileTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IPlainFileTask.class);

    private final String path;
    private final Long partitions;

    private final String delim;

    public IPlainFileTask(String name, IExecutor executor, String path, String delim) {
        this(name, executor, path, null, delim);
    }

    public IPlainFileTask(String name, IExecutor executor, String path, Long partitions, String delim) {
        super(name, executor, Mode.SAVE);
        this.path = path;
        this.partitions = partitions;
        this.delim = delim;
    }

    @Override
    public void contextError(IgnisException ex) throws IgnisException {
        throw ex;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "plainFile started");
        try {
            if (partitions == null) {
                executor.getIoModule().plainFile(path, delim);
            } else {
                executor.getIoModule().plainFile3(path, partitions, delim);
            }
        } catch (IExecutorException ex) {
            throw new IExecutorExceptionWrapper(ex);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "plainFile finished");
    }

}
