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

import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;

/**
 *
 * @author César Pomar
 */
public abstract class IExecutorStub {

    public static abstract class Factory {

        public abstract IExecutorStub getExecutorStub(long id, String type, IContainer container, IProperties properties);
    }

    protected final long id;
    protected final String type;
    protected final IContainer container;
    protected final IProperties properties;

    public IExecutorStub(long id, String type, IContainer container, IProperties properties) {
        this.id = id;
        this.type = type;
        this.container = container;
        this.properties = properties;
    }

    public String getType() {
        return type;
    }

    public abstract boolean isRunning();

    public abstract void test() throws IgnisException;

    public abstract void create() throws IgnisException;

    public abstract void destroy() throws IgnisException;

}
