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

import org.apache.thrift.transport.TTransport;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;

/**
 *
 * @author César Pomar
 */
public abstract class IContainerStub {

    public static abstract class Factory {

        public abstract IContainerStub getContainerStub(IProperties properties);
        
    }

    protected final IProperties properties;

    public IContainerStub(IProperties properties) {
        this.properties = properties;
    }

    public abstract boolean isRunning();

    public abstract String getHost();

    public abstract int getPortAlias(int port);

    public abstract TTransport getTransport();

    public abstract void test() throws IgnisException;

    public abstract void create() throws IgnisException;

    public abstract void destroy() throws IgnisException;

}
