/*
 * Copyright (C) 2019 César Pomar
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
package org.ignis.backend.scheduler;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ignis.backend.exception.IPropertyException;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.scheduler.model.IBind;
import org.ignis.backend.scheduler.model.INetwork;
import org.ignis.backend.scheduler.model.IVolume;

/**
 *
 * @author César Pomar
 */
public class ISchedulerParser {

    public static INetwork parseNetwork(IProperties props, String prefix) {
        INetwork network = new INetwork();
        Collection<String> ports = props.getKeysPrefix(prefix + ".");
        
        int transportPorts = props.getInteger(IKeys.TRANSPORT_PORTS);
        network.getTcpPorts().addAll(Collections.nCopies(transportPorts, 0));

        for (String key : ports) {
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

            if (type.equals("tcp")) {
                if (continerPort == 0) {
                    network.getTcpPorts().add(continerPort);
                } else {
                    network.getTcpMap().put(continerPort, hostPort);
                }
            } else {
                if (continerPort == 0) {
                    network.getUdpPorts().add(continerPort);
                } else {
                    network.getUdpMap().put(continerPort, hostPort);
                }
            }

        }

        ports = props.getKeysPrefix(prefix + "s.");

        for (String key : ports) {
            String type = key.substring((prefix + "s.").length()).toLowerCase();
            if (!type.equals("tcp") && !type.equals("udp")) {
                throw new IPropertyException(key, " expected type tcp/udp found " + type);
            }
            if (type.equals("tcp")) {
                network.getTcpPorts().addAll(props.getIntegerList(key));
            } else {
                network.getUdpPorts().addAll(props.getIntegerList(key));
            }
        }

        return network;
    }

    public static List<IBind> parseBinds(IProperties props, String prefix) {
        return null;//TODO
    }

    public static List<IVolume> parseVolumes(IProperties props, String prefix) {
        return null;//TODO
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
