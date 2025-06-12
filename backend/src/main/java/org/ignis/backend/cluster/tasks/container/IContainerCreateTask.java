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
import org.ignis.scheduler.IScheduler;
import org.ignis.scheduler.ISchedulerException;
import org.ignis.scheduler.model.IClusterInfo;
import org.ignis.scheduler.model.IClusterRequest;
import org.ignis.scheduler.model.IContainerInfo;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author César Pomar
 */
public final class IContainerCreateTask extends IContainerTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IContainerCreateTask.class);

    public static class Shared {

        public Shared(List<IContainer> containers) {
            this.containers = containers;
            this.barrier = new IBarrier(containers.size());
            this.alive = new AtomicInteger();
        }

        private final IBarrier barrier;
        private final List<IContainer> containers;
        private final AtomicInteger alive;
        private IClusterInfo clusterInfo;

    }

    private final Shared shared;
    private final IScheduler scheduler;
    private final IClusterRequest request;

    public IContainerCreateTask(String name, IContainer container, IScheduler scheduler, IClusterRequest request,
                                Shared shared) {
        super(name, container);
        this.shared = shared;
        this.scheduler = scheduler;
        this.request = request;
    }

    private void checkContainers() throws BrokenBarrierException, InterruptedException, ISchedulerException, IgnisException {
        String jobID = container.getProperties().getProperty(IKeys.JOB_ID);
        boolean ok = false;
        if (shared.barrier.await() == 0) {
            shared.alive.set(0);
        }
        shared.barrier.await();

        if (container.testConnection()) {
            ok = true;
        } else {
            LOGGER.info(log() + "Container connection lost");
            if (scheduler.getContainerStatus(jobID, container.getInfo().id()).equals(IContainerInfo.IStatus.RUNNING)) {
                LOGGER.info(log() + "Container already running");
                try {
                    LOGGER.info(log() + "Reconnecting to the container");
                    container.connect();
                    ok = true;
                } catch (IgnisException ex) {
                    LOGGER.warn(log() + "Container dead");
                }
            }
        }

        if (ok) {
            shared.alive.incrementAndGet();
        }

        if (shared.alive.get() != shared.containers.size()) {
            if (shared.barrier.await() == 0) {
                LOGGER.info(log() + "Repairing Cluster");
                shared.clusterInfo = scheduler.repairCluster(jobID, shared.clusterInfo, request);
            }
        }

        if (!ok) {
            container.setInfo(shared.clusterInfo.containers().get((int) container.getId()));
            container.connect();
        }

    }

    private void createContainers() throws BrokenBarrierException, InterruptedException, ISchedulerException, IgnisException {
        String jobID = container.getProperties().getProperty(IKeys.JOB_ID);
        LOGGER.info(log() + "Container not found");
        if (shared.barrier.await() == 0) {
            LOGGER.info(log() + "Creating new Cluster");
            shared.clusterInfo = scheduler.createCluster(jobID, request);
        }
        shared.barrier.await();
        container.setInfo(shared.clusterInfo.containers().get((int) container.getId()));
        LOGGER.info(log() + "Connecting to the container");
        container.connect();
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        try {
            if (container.getInfo() == null) {
                createContainers();
            } else {
                checkContainers();
            }
            LOGGER.warn(log() + "Container ready");

            if (Boolean.getBoolean(IKeys.DEBUG)) {
                LOGGER.info("Debug:" + log() + " ExecutorEnvironment{\n" +
                        container.getTunnel().execute("ignis-run env", false)
                        + '}');
            }
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
