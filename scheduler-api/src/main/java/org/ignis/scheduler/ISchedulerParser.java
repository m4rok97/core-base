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
package org.ignis.scheduler;

import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.properties.IPropertyException;
import org.ignis.scheduler.model.IBind;
import org.ignis.scheduler.model.IPort;
import org.ignis.scheduler.model.IVolume;

import java.util.*;


/**
 * @author César Pomar
 */
public final class ISchedulerParser {

    private final IProperties props;

    public ISchedulerParser(IProperties props) {
        this.props = props;
    }

    public Map<String, String> schedulerParams() {
        Map<String, String> params = new HashMap<>();
        int plen = IKeys.SCHEDULER_PARAMS.length() + 1;
        for (String key : props.getPrefixKeys(IKeys.SCHEDULER_PARAMS)) {
            params.put(key.substring(plen), props.getProperty(key));
        }
        return params;
    }


    public List<IPort> ports(String prefix) {
        List<IPort> ports = new ArrayList<>();
        Collection<String> propsPorts = props.getPrefixKeys(prefix);

        int transportPorts = props.getInteger(IKeys.TRANSPORT_PORTS);
        ports.addAll(Collections.nCopies(transportPorts, new IPort(0, 0, "tcp")));

        for (String key : propsPorts) {
            String subkey = key.substring((prefix + ".").length());
            String[] portSpec = subkey.split("\\.");
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

    public List<IBind> binds(String prefix) {
        List<IBind> binds = new ArrayList<>();
        binds.add(IBind.builder()
                .hostPath(props.getString(IKeys.DFS_ID))
                .containerPath(props.getString(IKeys.DFS_HOME))
                .readOnly(false).build());

        for (String key : props.getPrefixKeys(prefix)) {
            String hostpath = props.getString(key);
            boolean ro = false;
            if (hostpath.endsWith(":ro")) {
                ro = true;
                hostpath = hostpath.substring(0, hostpath.length() - 3);
            }

            binds.add(IBind.builder()
                    .hostPath(hostpath)
                    .containerPath(key.substring(prefix.length() + 1))
                    .readOnly(ro).build());
        }

        return binds;
    }

    public List<IVolume> volumes(String prefix) {
        List<IVolume> volumes = new ArrayList<>();

        for (String key : props.getPrefixKeys(prefix)) {
            volumes.add(IVolume.builder()
                    .containerPath(key.substring(prefix.length() + 1))
                    .size(props.getSILong(key)).build());
        }
        return volumes;
    }

    public Map<String, String> env(String prefix) {
        Map<String, String> env = new HashMap<>();
        int plen = prefix.length() + 1;
        for (String key : props.getPrefixKeys(prefix)) {
            env.put(key.substring(plen), props.getString(key));
        }
        return env;
    }

}
