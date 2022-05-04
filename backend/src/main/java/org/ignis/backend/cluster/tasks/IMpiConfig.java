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

package org.ignis.backend.cluster.tasks;

import org.ignis.backend.cluster.IExecutor;
import org.ignis.properties.IKeys;
import org.ignis.scheduler.model.INetworkMode;
import org.ignis.scheduler.model.IPort;

import java.util.*;
import java.util.stream.Collectors;

public class IMpiConfig {

    public static Map<String, String> get(IExecutor executor) {
        Map<String, String> conf = new HashMap<>();
        if (executor.getProperties().getDouble(IKeys.TRANSPORT_CORES) > 0 && executor.getCores() > 1) {
            conf.put("MPI_THREAD_MULTIPLE", "1");
            conf.put("MPIR_CVAR_CH4_NUM_VCIS", String.valueOf(executor.getCores()));
        }
        if( executor.getContainer().getInfo().getNetworkMode().equals(INetworkMode.BRIDGE)){
            conf.put("MPICH_SERVICE", executor.getContainer().getInfo().getHost());
        }
        List<IPort> mpiPorts = getPorts(executor);
        conf.put("MPICH_LIST_PORTS",
                mpiPorts.stream().map((IPort p) -> String.valueOf(p.getContainerPort())).collect(Collectors.joining(" ")));
        return conf;
    }

    public static List<IPort> getPorts(IExecutor executor) {
        List<IPort> randomPorts;
        if (executor.getContainer().getInfo().getNetworkMode().equals(INetworkMode.BRIDGE)){
            Set<String> bussy = new HashSet<>();
            for (IPort port : executor.getContainer().getContainerRequest().getPorts()) {
                if (port.getContainerPort() != 0) {
                    bussy.add(port.getProtocol() + port.getContainerPort());
                }
            }

            randomPorts = new ArrayList<>();
            for (IPort port : executor.getContainer().getInfo().getPorts()) {
                if (!bussy.contains(port.getProtocol() + port.getContainerPort()) && port.getProtocol().equalsIgnoreCase("tcp")) {
                    randomPorts.add(port);
                }
            }
        } else {
            randomPorts = executor.getContainer().getInfo().getPorts();
        }

        int transportPorts = executor.getProperties().getInteger(IKeys.TRANSPORT_PORTS);
        return randomPorts.subList(0, transportPorts);
    }

}
