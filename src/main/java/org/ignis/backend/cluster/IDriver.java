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
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.scheduler.model.IContainerDetails;

/**
 * @author César Pomar
 */
public final class IDriver {

    private final ILock lock;
    private final IExecutor executor;
    private IContainerDetails info;

    public IDriver(int port, IProperties properties) {
        this.lock = new ILock(-1);
        IContainer dummy = new IContainer(0, -1, null, properties);
        this.executor = new IExecutor(0, 0, dummy, port, 1);
    }

    public void initInfo(IContainerDetails info) {
        if (this.info == null) {
            this.info = info;
        }
    }

    public IProperties getProperties() {
        return executor.getProperties();
    }

    public IContainerDetails getInfo() {
        return info;
    }

    public ILock getLock() {
        return lock;
    }

    public IExecutor getExecutor() {
        return executor;
    }

    public String getName() {
        return "driver";
    }

}
