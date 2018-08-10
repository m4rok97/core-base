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
import org.ignis.rpc.manager.IRegisterManager;

/**
 *
 * @author César Pomar
 */
public final class IManagerExecutorStub extends IExecutorStub {

    public final static class Factory extends IExecutorStub.Factory {

        @Override
        public IExecutorStub getExecutorStub(long id, String type, IContainer container, IProperties properties) {
            return new IManagerExecutorStub(id, type, container, properties);
        }
    }

    private final IRegisterManager.Iface manager;
    private boolean running;

    public IManagerExecutorStub(long id, String type, IContainer container, IProperties properties) {
        super(id, type, container, properties);
        this.manager = container.getRegisterManager();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void test() throws IgnisException {
        try {
            manager.test(id);
        } catch (TException ex) {
            throw new IgnisException("Fails to test container", ex);
        }
    }

    @Override
    public void create() throws IgnisException {
        try {
            manager.execute(id, type);
            running = true;
        } catch (TException ex) {
            throw new IgnisException("Fails to create container", ex);
        }
    }

    @Override
    public void destroy() throws IgnisException {
        try {
            manager.destroy(id);
            running = false;
        } catch (TException ex) {
            throw new IgnisException("Fails to destroy container", ex);
        }
    }

}
