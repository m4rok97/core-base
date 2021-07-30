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
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class IExecutorDestroyTask extends IExecutorTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IExecutorDestroyTask.class);

    public IExecutorDestroyTask(String name, IExecutor executor) {
        super(name, executor);
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        if (!executor.isConnected()) {
            return;
        }
        LOGGER.info(log() + "Destroying executor");
        try {
            executor.getExecutorServerModule().stop();
        } catch (TException ex) {
            LOGGER.warn(log() + "Executor stopped with errors: " + ex);
        }
        executor.disconnect();
        try {
            String killScript = "timeout 30 bash -c \"while kill -0 " + executor.getPid() +
                    "; do sleep 1; done\" || kill -9 " + executor.getPid();
            executor.getContainer().getTunnel().execute(killScript, false);
        } catch (IgnisException ex) {
            LOGGER.warn(log() + ex.toString());
        }
        executor.setPid(-1);
        LOGGER.info(log() + "Executor destroyed");
    }

}
