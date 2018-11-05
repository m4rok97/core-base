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

import org.apache.thrift.TException;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;

/**
 *
 * @author César Pomar
 */
public final class IExecutorStub{

    private final long id;
    private final String type;
    private final IContainer container;
    private final IProperties properties;
    private boolean running;

    public IExecutorStub(long id, String type, IContainer container, IProperties properties) {
        this.id = id;
        this.type = type;
        this.container = container;
        this.properties = properties;
    }

    public String getType() {
        return type;
    }

    public IProperties getProperties() {
        return properties;
    }

    public boolean isRunning() {
        return running;
    }

    public void test() throws IgnisException {
        try {
            container.getRegisterManager().test(id);
        } catch (TException ex) {
            throw new IgnisException("Fails to test executor", ex);
        }
    }

    public void create() throws IgnisException {
        try {
            container.getRegisterManager().execute(id, type);
            running = true;
        } catch (TException ex) {
            throw new IgnisException("Fails to create executor", ex);
        }
    }

    public void destroy() throws IgnisException {
        try {
            container.getRegisterManager().destroy(id);
            running = false;
        } catch (TException ex) {
            throw new IgnisException("Fails to destroy executor", ex);
        }
    }

}
