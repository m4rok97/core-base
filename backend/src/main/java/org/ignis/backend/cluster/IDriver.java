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
package org.ignis.backend.cluster;

import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.executor.IDriverConectionTask;
import org.ignis.backend.cluster.tasks.executor.IDriverDataTask;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.scheduler3.model.IContainerInfo;

/**
 * @author César Pomar
 */
public final class IDriver {

    private final ILock lock;
    private final IContainer dummy;
    private final IExecutor executor;
    private final ITaskGroup connection;

    public IDriver(IProperties props, IContainerInfo info) {
        this.lock = new ILock(-1);
        dummy = new IContainer(0, 0, null, props);
        dummy.setInfo(info);
        this.executor = new IExecutor(0, 0, dummy, 1);
        this.connection = new ITaskGroup.Builder().
                newTask(new IDriverConectionTask(getName(), executor)).
                newLock(lock).
                build();
    }

    public IProperties getProperties() {
        return executor.getProperties();
    }

    public IContainerInfo getInfo() {
        return dummy.getInfo();
    }

    public ITaskGroup driverConnection() {
        return connection;
    }

    public ITaskGroup driverConnectionWithData(long dataID) {
        return new ITaskGroup.Builder().
                newLock(lock).
                newTask(new IDriverDataTask(getName(), executor, dataID)).
                newDependency(connection).
                build();
    }

    public ILock getLock() {
        return lock;
    }

    public IExecutor getExecutor() {
        return executor;
    }

    public String getName() {
        return "0-" + getProperties().getProperty(IKeys.DRIVER_NAME);
    }

}
