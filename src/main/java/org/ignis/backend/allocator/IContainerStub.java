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
package org.ignis.backend.allocator;

import java.util.Map;
import org.apache.thrift.transport.TTransport;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;

/**
 *
 * @author César Pomar
 */
public abstract class IContainerStub {

    protected final IProperties properties;

    public IContainerStub(IProperties properties) {
        this.properties = properties;
    }

    public IProperties getProperties() {
        return properties;
    }

    public abstract boolean isRunning();

    public abstract String getHost();

    public final Integer getHostPort(int port){
        return getPorts().get(port);
    }
    
    public abstract Map<Integer,Integer> getPorts();
    
    public abstract void request() throws IgnisException;

    public abstract void destroy() throws IgnisException;

}
