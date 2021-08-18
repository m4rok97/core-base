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
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.scheduler.IScheduler;
import org.ignis.scheduler.ISchedulerException;
import org.ignis.scheduler.ISchedulerParser;
import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IContainerStatus;
import org.ignis.scheduler.model.IPort;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;

/**
 * @author César Pomar
 */
public final class IContainerCreateTask extends IContainerTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IContainerCreateTask.class);

    public static class Shared {

        public Shared(List<IContainer> containers) {
            this.containers = containers;
            this.barrier = new IBarrier(containers.size());
            this.status = new IContainerStatus[containers.size()];
        }

        private final IBarrier barrier;
        private final List<IContainer> containers;
        private final IContainerStatus[] status;

    }

    private final Shared shared;
    private final IScheduler scheduler;

    public IContainerCreateTask(String name, IContainer container, IScheduler scheduler, Shared shared) {
        super(name, container);
        this.shared = shared;
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
        builder.memory(props.getSILong(IKeys.EXECUTOR_MEMORY));
        builder.swappiness(props.contains(IKeys.EXECUTOR_SWAPPINESS) ? props.getInteger(IKeys.EXECUTOR_SWAPPINESS) : null);
        builder.command("ignis-server");
        builder.arguments(Arrays.asList(props.getString(IKeys.EXECUTOR_RPC_PORT)));
        ISchedulerParser parser = new ISchedulerParser(props);
        builder.schedulerParams(parser.schedulerParams());
        List<IPort> ports;
        builder.ports(ports = parser.ports(IKeys.EXECUTOR_PORT));
        builder.binds(parser.binds(IKeys.EXECUTOR_BIND));
        builder.volumes(parser.volumes(IKeys.EXECUTOR_VOLUME));
        builder.hostnames(props.getStringList(IKeys.SCHEDULER_DNS));
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

    @Override
    public void run(ITaskContext context) throws IgnisException {
        try {
            int containerId = (int) container.getId();
            boolean tested = false;
            if (container.getInfo() != null) {
                if (container.testConnection()) {
                    shared.status[containerId] = IContainerStatus.RUNNING;
                    tested = true;
                } else {
                    shared.status[containerId] = IContainerStatus.ERROR;
                }
                if (shared.barrier.await() == 0) {
                    List<String> ids = new ArrayList<>();
                    for (IContainer c : shared.containers) {
                        if (shared.status[(int) c.getId()] == IContainerStatus.ERROR) {
                            ids.add(c.getInfo().getId());
                        }
                    }
                    List<IContainerStatus> status = scheduler.getStatus(ids);
                    Iterator<IContainerStatus> it = status.iterator();
                    for (IContainer c : shared.containers) {
                        if (shared.status[(int) c.getId()] == IContainerStatus.ERROR) {
                            shared.status[(int) c.getId()] = it.next();
                        }
                    }
                }
                shared.barrier.await();
                if (!tested) {
                    switch (shared.status[containerId]) {
                        case DESTROYED:
                        case FINISHED:
                        case ERROR:
                            shared.status[containerId] = IContainerStatus.ERROR;
                            break;
                        default:
                            LOGGER.info(log() + "Container already running");
                            if (!container.testConnection()) {
                                LOGGER.info(log() + "Reconnecting to the container");
                                try {
                                    container.connect();
                                } catch (IgnisException ex) {
                                    LOGGER.warn(log() + "Container dead");
                                    shared.status[containerId] = IContainerStatus.ERROR;
                                }
                            }
                    }
                }
                if (shared.barrier.await() == 0) {
                    List<String> stopped = new ArrayList<>();
                    List<Integer> stoppedIndex = new ArrayList<>();
                    for (int i = 0; i < shared.containers.size(); i++) {
                        if (shared.status[i] == IContainerStatus.ERROR) {
                            stopped.add(shared.containers.get(i).getInfo().getId());
                            stoppedIndex.add(i);
                            shared.containers.get(i).setInfo(null);
                        }
                    }
                    if (stopped.size() == shared.containers.size()) {
                        LOGGER.info(log() + "All containers dead");
                        try {
                            scheduler.destroyExecutorInstances(stopped);
                        } catch (ISchedulerException ex) {
                        }
                    } else {
                        for (int i = 0; i < stoppedIndex.size(); i++) {
                            shared.containers.get(i).setInfo(scheduler.restartContainer(stopped.get(i)));
                            if (Boolean.getBoolean(IKeys.DEBUG)) {
                                LOGGER.info("Debug:" + log() + "[" + i + "]" + shared.containers.get(i).getInfo());
                            }
                        }
                    }
                }
                shared.barrier.await();
            }

            if (container.getInfo() != null) {
                return;
            }

            if (shared.barrier.await() == 0) {
                String group = container.getProperties().getString(IKeys.JOB_GROUP);
                List<String> ids = scheduler.createExecutorContainers(group, name, parseContainer(), shared.containers.size());
                List<IContainerInfo> details = scheduler.getExecutorContainers(ids);
                for (int i = 0; i < shared.containers.size(); i++) {
                    if (Boolean.getBoolean(IKeys.DEBUG)) {
                        LOGGER.info("Debug:" + log() + "[" + i + "]" + details.get(i));
                    }
                    shared.containers.get(i).setInfo(details.get(i));
                }
            }
            shared.barrier.await();

            LOGGER.info(log() + "Connecting to the container");
            container.connect();

            if (Boolean.getBoolean(IKeys.DEBUG) && containerId == 0) {
                LOGGER.info("Debug:" + log() + " ExecutorEnvironment{\n" +
                        container.getTunnel().execute("env", false)
                        + '}');
            }
            LOGGER.info(log() + "Containers ready");
        } catch (IgnisException ex) {
            shared.barrier.fails();
            throw ex;
        } catch (BrokenBarrierException ex) {
            //Other Task has failed
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
    }
}
