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
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.scheduler.model.IContainerDetails;

/**
 *
 * @author César Pomar
 */
public final class IContainer {

    private final long id;
    private final ITunnel tunnel;
    private final IProperties properties;
    private IContainerDetails info;

    public IContainer(long id, ITunnel tunnel, IProperties properties) throws IgnisException {
        this.id = id;
        this.tunnel = tunnel;
        this.properties = properties;
    }

    public long getId() {
        return id;
    }

    public ITunnel getTunnel() {
        return tunnel;
    }

    public IContainerDetails getInfo() {
        return info;
    }

    public void setInfo(IContainerDetails info) {
        this.info = info;
    }

    public IProperties getProperties() {
        return properties;
    }

    public boolean testConnection(){
        return tunnel.test();
    }
    
    public void connect() throws IgnisException {
        tunnel.open(info.getHost(), info.getNetwork().getTcpMap().get(properties.getInteger(IKeys.DRIVER_RPC_PORT)));
    }

    public IExecutor createExecutor(long worker) throws IgnisException{
        return new IExecutor(worker, this, tunnel.registerPort());
    }
}
