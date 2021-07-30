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
package org.ignis.backend.cluster;

import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.scheduler.model.IContainerInfo;

/**
 * @author César Pomar
 */
public final class IContainer {

    private final long id;
    private final long cluster;
    private final ITunnel tunnel;
    private final IProperties properties;
    private IContainerInfo info;
    private int resets;

    public IContainer(long id, long cluster, ITunnel tunnel, IProperties properties) {
        this.id = id;
        this.cluster = cluster;
        this.tunnel = tunnel;
        this.properties = properties;
        this.resets = -1;
    }

    public long getId() {
        return id;
    }

    public long getCluster() {
        return cluster;
    }

    public ITunnel getTunnel() {
        return tunnel;
    }

    public IContainerInfo getInfo() {
        return info;
    }

    public void setInfo(IContainerInfo info) {
        this.info = info;
        resets++;
    }

    public int getResets() {
        return resets;
    }

    public IProperties getProperties() {
        return properties;
    }

    public boolean testConnection() {
        return tunnel.test();
    }

    public void connect() throws IgnisException {
        tunnel.open(info.getHost(), info.searchHostPort(properties.getInteger(IKeys.EXECUTOR_RPC_PORT)));
    }

    public IExecutor createExecutor(long id, long worker, int cores) throws IgnisException {
        return new IExecutor(id, worker, this, tunnel.registerPort(), cores);
    }
}
