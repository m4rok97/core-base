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

import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.cluster.tasks.Task;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class IImportDataTask extends IExecutorTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(IImportDataTask.class);

    public static final boolean SEND = true;
    public static final boolean RECEIVE = false;

    public static class ImportDataShared {

    }

    public IImportDataTask(IExecutor executor, IBarrier barrier, IReduceByKeyTask.KeyShared keyShared, boolean type) {
        super(executor);
    }

    @Override
    public void execute() throws IgnisException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
