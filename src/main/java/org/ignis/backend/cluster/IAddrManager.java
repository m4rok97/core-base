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
import org.ignis.backend.properties.IPropsKeys;

/**
 *
 * @author César Pomar
 */
public class IAddrManager {

    public static final String SEPARATOR = "!";
    public static final String LOCAL = "local";
    public static final String SOCKET = "socket";
    public static final String UNIX_SOCKET = "unixSocket";
    public static final String MEMORY_BUFFER = "memoryBuffer";

    public IAddrManager() {
    }

    private int getSocketPort(IExecutor e) throws IgnisException {
        int port = e.getContainer().getProperties().getInteger(IPropsKeys.TRANSPORT_PORT);
        return e.getContainer().getExposePort(port);
    }

    private String joinFields(String... fields) {
        return String.join(SEPARATOR, fields);
    }

    private String parseLocal(IExecutor source, IExecutor target) {
        return LOCAL;
    }

    private String parseSocket(IExecutor source, IExecutor target) throws IgnisException {
        return joinFields(
                SOCKET,
                target.getContainer().getHost(),
                String.valueOf(getSocketPort(target))
        );
    }

    private String parseContainerUnixSocket(IExecutor source, IExecutor target) throws IgnisException {
        return parseSocket(source, target);//TODO
    }

    private String parseContainerMemoryBuffer(IExecutor source, IExecutor target) throws IgnisException {
        return joinFields(
                MEMORY_BUFFER,
                target.getContainer().getHost(),
                String.valueOf(getSocketPort(target)),
                "/dev/shm",
                String.valueOf(source.getContainer().getProperties().getSILong(IPropsKeys.TRANSPORT_BUFFER))
        );
    }

    private String parseHostUnixSocket(IExecutor source, IExecutor target) throws IgnisException {
        return parseSocket(source, target);//TODO
    }

    private String parseHostMemoryBuffer(IExecutor source, IExecutor target) throws IgnisException {
        return parseSocket(source, target);//TODO
    }

    public String parseAddr(IExecutor source, IExecutor target) throws IgnisException {
        if (source.equals(target)) {
            return parseLocal(source, target);
        }
        String type = source.getProperties().getProperty(IPropsKeys.TRANSPORT_TYPE);
        if (source.getContainer().equals(target.getContainer())) {
            if (MEMORY_BUFFER.equals(type)) {
                parseContainerMemoryBuffer(source, target);
            } else if (UNIX_SOCKET.equals(type)) {
                parseContainerUnixSocket(source, target);
            }
        }

        if (source.getContainer().getHost().equals(target.getContainer().getHost())) {
            if (MEMORY_BUFFER.equals(type)) {
                parseHostMemoryBuffer(source, target);
            } else if (UNIX_SOCKET.equals(type)) {
                parseHostUnixSocket(source, target);
            }
        }
        return parseSocket(source, target);
    }

}
