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
package org.ignis.backend.cluster.helpers.cluster;

import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.ITunnel;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.container.IContainerCreateTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.scheduler3.IScheduler;
import org.ignis.scheduler3.ISchedulerParser;
import org.ignis.scheduler3.model.IClusterRequest;
import org.ignis.scheduler3.model.IContainerInfo;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author César Pomar
 */
public final class IClusterCreateHelper extends IClusterHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IClusterCreateHelper.class);

    public IClusterCreateHelper(ICluster cluster, IProperties properties) {
        super(cluster, properties);
    }

    public ITaskGroup create(IScheduler scheduler) throws IgnisException {
        int instances = properties.getInteger(IKeys.EXECUTOR_INSTANCES);
        if (instances < 1) {
            throw new IgnisException("Executor instances must be greater than zero");
        }
        ITaskGroup.Builder builder = new ITaskGroup.Builder(cluster.getLock());
        LOGGER.info(log() + "Registering cluster with " + instances + " containers");

        for (int i = 0; i < instances; i++) {
            IContainer container = new IContainer(i, cluster.getId(), createTunnel(), properties);
            cluster.getContainers().add(container);
        }

        var shared = new IContainerCreateTask.Shared(cluster.getContainers());
        var clusterRequest = createClusterRequest();
        for (int i = 0; i < instances; i++) {
            builder.newTask(new IContainerCreateTask(getName(), cluster.getContainers().get(i), scheduler, clusterRequest, shared));
        }

        return builder.build();
    }

    private ITunnel createTunnel() {
        return new ITunnel(
                properties.getProperty(IKeys.CRYPTO_$PRIVATE$),
                properties.getProperty(IKeys.CRYPTO_PUBLIC)
        );
    }

    private IClusterRequest createClusterRequest() {
        var parser = new ISchedulerParser(properties);
        int port;
        if (parser.networkMode().equals(IContainerInfo.INetworkMode.BRIDGE)) {
            port = properties.getInteger(IKeys.PORT);

            var lprops = parser.getProperties();
            lprops.setProperty(IProperties.join(IKeys.EXECUTOR_PORTS, "tcp", lprops.getString(IKeys.PORT)), "0");
            var key = IProperties.join(IKeys.EXECUTOR_PORTS, "tcp", "host");
            var ports = lprops.hasProperty(key) ? lprops.getStringList(key) : new ArrayList<String>();
            ports.addAll(Collections.nCopies(lprops.getInteger(IKeys.TRANSPORT_PORTS), "0"));
            lprops.setList(key, ports);
        } else {
            port = 0;
        }
        var request = parser.parse(IKeys.EXECUTOR, cluster.getName(),
                List.of("ignis-sshserver", "executor", String.valueOf(port)));
        parser.containerEnvironment(request);
        return request;
    }

}
