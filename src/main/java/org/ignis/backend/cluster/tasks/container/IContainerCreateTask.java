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
package org.ignis.backend.cluster.tasks.container;

import org.apache.thrift.TException;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.IExecutionContext;
import org.ignis.backend.cluster.helpers.IHelper;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IContainerCreateTask extends IContainerTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IContainerCreateTask.class);

    public IContainerCreateTask(IHelper helper, IContainer container) {
        super(helper, container);
    }

    @Override
    public void execute(IExecutionContext context) throws IgnisException {
        if (container.getStub().isRunning()) {
            try {
                container.getServerManager().test();
                LOGGER.info(log() + "Container already running");
                return;
            } catch (TException ex) {
                LOGGER.warn(log() + "Container dead");
            }
        }
        LOGGER.info(log() + "Starting new container");
        container.getStub().request();
        LOGGER.info(log() + "Connecting to the container");
        container.connect();
    }
}
