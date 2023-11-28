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
import org.ignis.backend.cluster.ISSH;
import org.ignis.backend.cluster.tasks.ITaskGroup;
import org.ignis.backend.cluster.tasks.container.IContainerCreateTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.scheduler.IScheduler;
import org.ignis.scheduler.ISchedulerParser;
import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IPort;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author César Pomar
 */
public final class IClusterCreateHelper extends IClusterHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IClusterCreateHelper.class);

    public IClusterCreateHelper(ICluster cluster, IProperties properties) {
        super(cluster, properties);
    }

    public ITaskGroup create(IScheduler scheduler, ISSH ssh) throws IgnisException {
        int instances = properties.getInteger(IKeys.EXECUTOR_INSTANCES);
        if (instances < 1) {
            throw new IgnisException("Executor instances must be greater than zero");
        }
        ITaskGroup.Builder builder = new ITaskGroup.Builder(cluster.getLock());
        LOGGER.info(log() + "Registering cluster with " + instances + " containers");
        IContainerInfo containerRequest = parseContainerRequest();

        for (int i = 0; i < instances; i++) {
            IContainer container = new IContainer(i, cluster.getId(), ssh.createTunnel(), properties, containerRequest);
            cluster.getContainers().add(container);
        }

        IContainerCreateTask.Shared shared = new IContainerCreateTask.Shared(cluster.getContainers());
        for (int i = 0; i < instances; i++) {
            builder.newTask(new IContainerCreateTask(getName(), cluster.getContainers().get(i), scheduler, containerRequest, shared));
        }

        return builder.build();
    }

    private IContainerInfo parseContainerRequest() {
        IProperties props = properties;
        IContainerInfo.IContainerInfoBuilder builder = IContainerInfo.builder();

        if (props.contains(IKeys.REGISTRY)) {
            String registry = props.getProperty(IKeys.REGISTRY);
            if (!registry.endsWith("/")) {
                registry += "/";
            }
            builder.image(registry + props.getProperty(IKeys.EXECUTOR_IMAGE));
        } else {
            builder.image(props.getProperty(IKeys.EXECUTOR_IMAGE));
        }
        builder.cpus(props.getInteger(IKeys.EXECUTOR_CORES));
        builder.memory(props.getSILong(IKeys.EXECUTOR_MEMORY));
        builder.command("ignis-server");
        builder.arguments(List.of(props.getString(IKeys.EXECUTOR_RPC_PORT), "0"));
        ISchedulerParser parser = new ISchedulerParser(props);
        builder.schedulerParams(parser.schedulerParams());
        List<IPort> ports;
        builder.ports(ports = parser.ports(IKeys.EXECUTOR_PORT));
        builder.binds(parser.binds(IKeys.EXECUTOR_BIND));
        builder.volumes(parser.volumes(IKeys.EXECUTOR_VOLUME));
        if(props.contains(IKeys.SCHEDULER_DNS)){
            builder.hostnames(props.getStringList(IKeys.SCHEDULER_DNS));
        }
        Map<String, String> env = parser.env(IKeys.EXECUTOR_ENV);
        env.put("IGNIS_DRIVER_PUBLIC_KEY", props.getString(IKeys.DRIVER_PUBLIC_KEY));
        env.put("IGNIS_DRIVER_HEALTHCHECK_INTERVAL", String.valueOf(props.getInteger(IKeys.DRIVER_HEALTHCHECK_INTERVAL)));
        env.put("IGNIS_DRIVER_HEALTHCHECK_TIMEOUT", String.valueOf(props.getInteger(IKeys.DRIVER_HEALTHCHECK_TIMEOUT)));
        env.put("IGNIS_DRIVER_HEALTHCHECK_RETRIES", String.valueOf(props.getInteger(IKeys.DRIVER_HEALTHCHECK_RETRIES)));
        env.put("IGNIS_DRIVER_HEALTHCHECK_URL", props.getString(IKeys.DRIVER_HEALTHCHECK_URL));

        builder.environmentVariables(env);
        if (props.contains(IKeys.EXECUTOR_HOSTS)) {
            builder.preferedHosts(props.getStringList(IKeys.EXECUTOR_HOSTS));
        }

        ports.add(new IPort(props.getInteger(IKeys.EXECUTOR_RPC_PORT), 0, "tcp"));

        return builder.build();
    }

}
