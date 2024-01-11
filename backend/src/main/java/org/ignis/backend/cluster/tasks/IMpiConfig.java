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
import org.ignis.properties.IProperties;
import org.ignis.scheduler3.model.IContainerInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IMpiConfig {

    public static Map<String, String> getEnv(IExecutor executor) {
        Map<String, String> env = new HashMap<>();
        if (executor.getProperties().getDouble(IKeys.TRANSPORT_CORES) > 0 && executor.getCores() > 1) {
            env.put("MPI_THREAD_MULTIPLE", "1");
            env.put("MPIR_CVAR_CH4_NUM_VCIS", String.valueOf(executor.getCores()));
        }
        if (executor.getContainer().getInfo().network().equals(IContainerInfo.INetworkMode.BRIDGE)) {
            env.put("MPICH_SERVICE", executor.getContainer().getInfo().node());
            int n = executor.getProperties().getInteger(IKeys.TRANSPORT_PORTS);

            List<String> ports = executor.getContext().getStringList(
                    IProperties.join(IKeys.EXECUTOR_PORTS, "tcp", "host"));

            env.put("MPICH_LIST_PORTS", String.join(" ", ports.subList(ports.size() - n, ports.size())));
        }
        return env;
    }

}
