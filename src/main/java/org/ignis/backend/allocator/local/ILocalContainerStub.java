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
package org.ignis.backend.allocator.local;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.ignis.backend.allocator.IContainerStub;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.properties.IPropsKeys;

/**
 *
 * @author César Pomar
 */
public final class ILocalContainerStub extends IContainerStub {

    private final Map<Integer, Integer> ports;
    private Process manager;

    public ILocalContainerStub(IProperties properties) throws IgnisException {
        super(properties);
        ports = new HashMap<>();
        int port;
        ports.put(port = properties.getInteger(IPropsKeys.MANAGER_RPC_PORT), port);
        ports.put(port = properties.getInteger(IPropsKeys.TRANSPORT_PORT), port);
    }

    @Override
    public boolean isRunning() {
        return manager != null;
    }

    @Override
    public String getHost() {
        return "localhost";
    }

    @Override
    public Map<Integer, Integer> getPorts() {
        return Collections.unmodifiableMap(ports);
    }

    @Override
    public void request() throws IgnisException {
        try {
            manager = new ProcessBuilder("ignis-manager",
                    String.valueOf(properties.getInteger(IPropsKeys.MANAGER_RPC_PORT)),
                    String.valueOf(properties.getInteger(IPropsKeys.MANAGER_EXECUTORS_PORT))
            ).redirectError(Redirect.INHERIT).redirectOutput(Redirect.INHERIT).start();
        } catch (IOException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroy() throws IgnisException {
        manager.destroyForcibly();
        manager = null;
    }

}
