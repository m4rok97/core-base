/*
 *
 *  * Copyright (C) 2019 César Pomar
 *  *
 *  * This program is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * This program is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package org.ignis.properties;

import org.ignis.scheduler.model.IBind;
import org.ignis.scheduler.model.IPort;
import org.ignis.scheduler.model.IVolume;

import java.util.*;

/**
 * @author César Pomar
 */
public class IPropetiesParser {

    public static List<IPort> parsePorts(IProperties props, String prefix) {
        List<IPort> ports = new ArrayList<>();
        Collection<String> propsPorts = props.getKeysPrefix(prefix + ".");

        int transportPorts = props.getInteger(IKeys.TRANSPORT_PORTS);
        ports.addAll(Collections.nCopies(transportPorts, new IPort(0, 0, "tcp")));

        for (String key : propsPorts) {
            String subkey = key.substring((prefix + ".").length());
            String[] portSpec = subkey.split(".");
            if (portSpec.length != 2) {
                throw new IPropertyException(key, " has bad format, use *.{type}.{continer_port}={host_port}");
            }

            String type = portSpec[0].toLowerCase();
            if (!type.equals("tcp") && !type.equals("udp")) {
                throw new IPropertyException(key, " expected type tcp/udp found " + type);
            }

            int continerPort = Integer.parseInt(portSpec[1]);
            int hostPort = props.getInteger(key);
            ports.add(new IPort(continerPort, hostPort, type));
        }

        if (props.contains(prefix + "s.tcp")) {
            for (Integer port : props.getIntegerList(prefix + "s.tcp")) {
                ports.add(new IPort(port, 0, "tcp"));
            }
        }

        if (props.contains(prefix + "s.udp")) {
            for (Integer port : props.getIntegerList(prefix + "s.udp")) {
                ports.add(new IPort(port, 0, "tcp"));
            }
        }

        return ports;
    }

    public static List<IBind> parseBinds(IProperties props, String prefix) {//TODO User volumes
        List<IBind> binds = new ArrayList<>();
        binds.add(IBind.builder()
                .hostPath(props.getString(IKeys.DFS_ID))
                .containerPath(props.getString(IKeys.DFS_HOME))
                .readOnly(false).build());
        return binds;
    }

    public static List<IVolume> parseVolumes(IProperties props, String prefix) {//TODO User binds
        return new ArrayList<>();
    }

    public static Map<String, String> parseEnv(IProperties props, String prefix) {
        Collection<String> keys = props.getKeysPrefix(prefix + ".");
        Map<String, String> env = new HashMap<>(keys.size());
        for (String key : keys) {
            String subkey = key.substring((prefix + ".").length());
            env.put(subkey, props.getString(key));
        }
        return env;
    }

}
