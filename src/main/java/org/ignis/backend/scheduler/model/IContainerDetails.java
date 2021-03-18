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
package org.ignis.backend.scheduler.model;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 *
 * @author César Pomar
 */
@Getter
@Builder
@AllArgsConstructor
public class IContainerDetails {

    public static enum ContainerStatus {
        ACCEPTED,
        RUNNING,
        ERROR,
        FINISHED,
        DESTROYED,
        UNKNOWN
    }

    private final String id;
    private final String host;
    private final String image;
    private final String command;
    private final List<String> arguments;
    private final int cpus;
    private final long memory;//MiB
    private final Integer swappiness;
    private final List<IPort> ports;
    private final List<IBind> binds;
    private final List<IVolume> volumes;
    private final List<String> preferedHosts;
    private final Map<String, String> environmentVariables;

    public Integer searchHostPort(Integer containerPort) {
        for (IPort port : ports) {
            if (containerPort.equals(port.getContainerPort())) {
                return port.getHostPort();
            }
        }
        return null;
    }
}
