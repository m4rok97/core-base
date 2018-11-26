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

    private String parseLocal(IExecutor source, IExecutor target) {
        return LOCAL;
    }

    private String parseSocket(IExecutor source, IExecutor target) throws IgnisException {
        StringBuilder addr = new StringBuilder();
         int port = target.getContainer().getProperties().getInteger(IPropsKeys.TRANSPORT_PORT);
        addr.append(SOCKET).append(SEPARATOR);
        addr.append(target.getContainer().getHost()).append(SEPARATOR);
        addr.append(target.getContainer().getExposePort(port));
        
        return addr.toString();
    }

    private String parseUnixSocket(IExecutor source, IExecutor target) throws IgnisException{
        return null;
    }

    private String parseMemoryBuffer(IExecutor source, IExecutor target) throws IgnisException{
        return null;
    }

    public String parseAddr(IExecutor source, IExecutor target) throws IgnisException{
        if (source.equals(target)) {
            return parseLocal(source, target);
        } else {
            return parseSocket(source, target);
        }
    }

}
