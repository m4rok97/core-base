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
package org.ignis.backend.cluster.tasks.container;

import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.scheduler.IScheduler;
import org.ignis.properties.IPropetiesParser;
import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IPort;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author César Pomar
 */
public final class IContainerCreateTask extends IContainerTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IContainerCreateTask.class);

    private final List<IContainer> containers;
    private final IScheduler scheduler;

    public IContainerCreateTask(String name, IContainer container, IScheduler scheduler, List<IContainer> containers) {
        super(name, container);
        this.containers = containers;
        this.scheduler = scheduler;
    }

    private IContainerInfo parseContainer() {
        IProperties props = container.getProperties();
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
        builder.memory((long) Math.ceil(props.getSILong(IKeys.EXECUTOR_MEMORY) / 1024 / 1024));
        builder.shm(props.contains(IKeys.EXECUTOR_SHM) ? (long) Math.ceil(props.getSILong(IKeys.EXECUTOR_SHM) / 1024 / 1024) : null);
        builder.swappiness(props.contains(IKeys.EXECUTOR_SWAPPINESS) ? props.getInteger(IKeys.EXECUTOR_SWAPPINESS) : null);
        builder.command("ignis-server");
        builder.arguments(Arrays.asList(props.getString(IKeys.EXECUTOR_RPC_PORT)));
        List<IPort> ports;
        builder.ports(ports = IPropetiesParser.parsePorts(props, IKeys.EXECUTOR_PORT));
        builder.binds(IPropetiesParser.parseBinds(props, IKeys.EXECUTOR_BIND));
        builder.volumes(IPropetiesParser.parseVolumes(props, IKeys.EXECUTOR_VOLUME));
        builder.hostnames(props.getStringList(IKeys.SCHEDULER_DNS));
        Map<String, String> env = IPropetiesParser.parseEnv(props, IKeys.EXECUTOR_ENV);
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

    @Override
    public void run(ITaskContext context) throws IgnisException {
        List<IContainer> stopped = new ArrayList<>();
        boolean news = false;
        for (int i = 0; i < containers.size(); i++) {
            if (containers.get(i).getInfo() == null) {
                news = true;
                break;
            }
            switch (scheduler.getStatus(containers.get(i).getInfo().getId())) {
                case DESTROYED:
                case FINISHED:
                case ERROR:
                    stopped.add(containers.get(i));
                    break;
                default:
                    LOGGER.info(log() + "Container " + i + " already running");
                    if (!containers.get(i).testConnection()) {
                        LOGGER.info(log() + "Reconnecting to the container " + i);
                        try {
                            containers.get(i).connect();
                        } catch (IgnisException ex) {
                            LOGGER.warn(log() + "Container " + i + " dead");
                            stopped.add(containers.get(i));
                        }
                    }
            }
        }

        if (stopped.isEmpty() && !news) {
            return;
        }
        LOGGER.info(log() + "Starting new containers");

        if (news || stopped.size() == containers.size()) {
            String group = container.getProperties().getString(IKeys.JOB_GROUP);
            List<String> ids = scheduler.createExecutorContainers(group, name, parseContainer(), containers.size());
            List<IContainerInfo> details = scheduler.getContainers(ids);
            for (int i = 0; i < containers.size(); i++) {
                if (Boolean.getBoolean(IKeys.DEBUG)) {
                    LOGGER.info("Debug:" + log() + "[" + i + "]" + details.get(i));
                }
                containers.get(i).setInfo(details.get(i));
            }
        } else {
            for (IContainer container : stopped) {
                container.setInfo(scheduler.restartContainer(container.getInfo().getId()));
                if (Boolean.getBoolean(IKeys.DEBUG)) {
                    LOGGER.info("Debug:" + log() + "[" + container.getId() + "]" + container.getInfo());
                }
            }
        }

        LOGGER.info(log() + "Connecting to the containers");
        for (int i = 0; i < containers.size(); i++) {
            containers.get(i).connect();
        }

        if (Boolean.getBoolean(IKeys.DEBUG)) {
            LOGGER.info("Debug:" + log() + " ExecutorEnvironment{\n" +
                    containers.get(0).getTunnel().execute("env", false)
                    + '}');
        }
        LOGGER.info(log() + "Containers ready");
    }
}