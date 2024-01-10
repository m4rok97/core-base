/*
 *
 *  * Copyright (C) 2019 César Pomar
 *  *
 *  * This program is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * This program is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package org.ignis.backend.cluster.tasks.executor;

import org.apache.thrift.TException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.IMpiConfig;
import org.ignis.backend.exception.IExecutorExceptionWrapper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IKeys;
import org.ignis.rpc.IExecutorException;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * @author César Pomar
 */
public class IDriverConectionTask extends IExecutorTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IDriverConectionTask.class);

    public IDriverConectionTask(String name, IExecutor executor) {
        super(name, executor);
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        if (executor.isConnected()) {
            try {
                executor.getExecutorServerModule().test();
                return;
            } catch (Exception ex) {
                LOGGER.warn(log() + "driver conection lost");
            }
        }
        LOGGER.warn(log() + "connecting to the driver");

        for (int i = 0; i < 300; i++) {
            try {
                executor.connect(Path.of(executor.getProperties().getProperty(IKeys.JOB_SOCKETS), "driver.sock").toString());
                break;
            } catch (TException ex) {
                if (i == 299) {
                    throw new IgnisException(ex.getMessage(), ex);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex1) {
                    throw new IgnisException(ex.getMessage(), ex);
                }
            }
        }

        try {
            executor.getExecutorServerModule().test();
            executor.getExecutorServerModule().start(executor.getContext().toMap(true), IMpiConfig.getEnv(executor));
        } catch (IExecutorException ex) {
            throw new IExecutorExceptionWrapper(ex);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

}
