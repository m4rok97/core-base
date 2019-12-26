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

/**
 *
 * @author César Pomar
 */
public final class IDriver {

    private final int port;
    private final ILock lock;
    private final IExecutor executor;

    public IDriver(int port) {
        this.port = port;
        this.lock = new ILock(-1);
        this.executor = new IExecutor(0, null, port);
    }

    public int getPort() {
        return port;
    }

    public ILock getLock() {
        return lock;
    }

    public IExecutor getExecutor() {
        return executor;
    }
    
    public String getName(){
        return "driver";
    }

}
