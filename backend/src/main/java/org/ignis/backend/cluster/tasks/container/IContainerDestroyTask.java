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
import org.ignis.scheduler3.IScheduler;
import org.ignis.scheduler3.ISchedulerException;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;

/**
 * @author César Pomar
 */
public final class IContainerDestroyTask extends IContainerTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IContainerDestroyTask.class);

    public static class Shared {

        public Shared(int containers) {
            this.barrier = new IBarrier(containers);
        }

        private final IBarrier barrier;

    }

    private final Shared shared;
    private final IScheduler scheduler;
    private final String clusterID;

    public IContainerDestroyTask(String name, IContainer container, IScheduler scheduler, String clusterID, Shared shared) {
        super(name, container);
        this.scheduler = scheduler;
        this.clusterID = clusterID;
        this.shared = shared;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        try {
            String jobID = container.getProperties().getProperty(IKeys.JOB_ID);
            if (container.getInfo() == null) {
                if (Boolean.getBoolean(IKeys.DEBUG)) {
                    LOGGER.info(log() + "Container not started");
                }
                return;
            }
            if (Boolean.getBoolean(IKeys.DEBUG)) {
                try {
                    switch (scheduler.getContainerStatus(jobID, container.getInfo().id())) {
                        case ACCEPTED:
                            LOGGER.info(log() + "Container is not launched yet");
                            break;
                        case RUNNING:
                            LOGGER.info(log() + "Container is running");
                            break;
                        case ERROR:
                            LOGGER.info(log() + "Container has an error");
                            break;
                        case FINISHED:
                            LOGGER.info(log() + "Container is finished");
                            break;
                        case DESTROYED:
                            LOGGER.info(log() + "Container already destroyed");
                            return;
                        case UNKNOWN:
                            LOGGER.info(log() + "Container has a unknown status");
                            break;
                    }
                } catch (ISchedulerException ex) {
                    LOGGER.info(log(), ex);
                }
            }

            container.getTunnel().close();
            if (shared.barrier.await() == 0) {
                try {
                    scheduler.destroyCluster(jobID, clusterID);
                } catch (ISchedulerException ex) {
                    LOGGER.warn("Containers destroyed with errors", ex);
                }
            }
            shared.barrier.await();
            LOGGER.info(log() + "Container destroyed");
        } catch (BrokenBarrierException ex) {
            //Other Task has failed
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        } finally {
            container.setInfo(null);
        }
    }

}
