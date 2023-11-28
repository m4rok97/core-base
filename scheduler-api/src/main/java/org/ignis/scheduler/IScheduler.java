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
package org.ignis.scheduler;

import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IContainerStatus;

import java.util.List;

/**
 * @author César Pomar
 */
public interface IScheduler {

    /**
     * Creates a group, and return its id. Scheduler must identify all containers within a group by
     * group id. The param <code>name</code> is for display purposes and can be ignored by the scheduler.
     *
     * @param name Group name
     * @return Group id
     * @throws ISchedulerException Scheduler fails
     */
    String createGroup(String name) throws ISchedulerException;

    /**
     * Destroys a group, all containers within a group must be destroyed before destroying the group.
     *
     * @param group Group id
     * @throws ISchedulerException Scheduler fails
     */
    void destroyGroup(String group) throws ISchedulerException;

    /**
     * Create a single container for driver within a group. The param <code>name</code> is for display purposes
     * and can be ignored by the scheduler. Container id is unique and identify the container and the group.
     *
     * @param group     Group id
     * @param name      Driver container name
     * @param container Request resources
     * @return Container id
     * @throws ISchedulerException Scheduler fails
     */
    String createDriverContainer(String group, String name, IContainerInfo container) throws ISchedulerException;

    /**
     * Create a multiple instances of a container for a set of executors. The param <code>name</code> is for display
     * purposes and can be ignored by the scheduler. Container id is unique and identify the container and the group.
     *
     * @param group     Group id
     * @param name      Executor containers name
     * @param container Request resources for each container.
     * @param instances Number of containers
     * @return List of container IDs with size <code>instances</code>
     * @throws ISchedulerException Scheduler fails
     */
    List<String> createExecutorContainers(String group, String name, IContainerInfo container, int instances) throws ISchedulerException;

    /**
     *
     * Executors container info for <code>createDriverWithExecutorContainers</code>
     *
     * @param name      Executor containers name
     * @param container Request resources for each container.
     * @param instances Number of containers
     */
    record ExecutorContainers(String name, IContainerInfo container, int instances) {
    }

    /**
     * Creates a single container (<code>createDriverContainer</code>) for both driver and executor containers
     * (<code>createExecutorContainers</code>) in a single call.
     * <p>
     * This method is used when <code>isDynamic()</code> returns false. When a scheduler doesn't support allocating
     * resources dynamically, it can allocate all resources at the same time. Otherwise, a scheduler with dynamic
     * support can use this method to reserve all resources at once and avoid future allocations.
     *
     * @param group              Group id
     * @param driverName         Driver name
     * @param driverContainer    Request resources for driver container.
     * @param executorContainers Request resources for different executor container instances
     * @return Driver container id
     * @throws ISchedulerException Scheduler fails
     */
    String createDriverWithExecutorContainers(String group, String driverName, IContainerInfo driverContainer,
                                              List<ExecutorContainers> executorContainers) throws ISchedulerException;

    /**
     * Get the state of a container. The function works with driver container or an executor instance.
     *
     * @param id Container id
     * @return Container status.
     * @throws ISchedulerException Scheduler fails
     */
    IContainerStatus getStatus(String id) throws ISchedulerException;

    /**
     * Get the state of multiple containers. The function works with driver container or a subgroup of
     * executor instance. It can be implemented as multiple calls to <code>getStatus(String id)</code>.
     *
     * @param ids List of container ids
     * @return List of Container status.
     * @throws ISchedulerException Scheduler fails
     */
    List<IContainerStatus> getStatus(List<String> ids) throws ISchedulerException;

    /**
     * Get a container. Container info must contain at least the same information that was used to request the container.
     * The function works with driver container or an executor instance.
     *
     * @param id Container id
     * @return Container info
     * @throws ISchedulerException Scheduler fails
     */
    IContainerInfo getContainer(String id) throws ISchedulerException;

    /**
     * Gets multiple executors instances.
     * The param <code>ids</code> must be a sublist of <code>createExecutorContainers</code>
     *
     * @param ids List of container ids
     * @return List of container IDs with size <code>ids.size()</code>
     * @throws ISchedulerException Scheduler fails
     */
    List<IContainerInfo> getExecutorContainers(List<String> ids) throws ISchedulerException;

    /**
     * Restarts a container.
     * If the container id is an executor container instance, restarting the container will not affect the others.
     *
     * @param id Container id
     * @throws ISchedulerException Scheduler fails
     */
    IContainerInfo restartContainer(String id) throws ISchedulerException;

    /**
     * Destroys a driver container. Executor container instance doesn't have to be supported.
     *
     * @param id Container id
     * @throws ISchedulerException Scheduler fails
     */
    void destroyDriverContainer(String id) throws ISchedulerException;

    /**
     * Destroys all executor instances. Destroying a subgroup of executors instances doesn't have to be supported.
     *
     * @param ids List of container IDs
     * @throws ISchedulerException Scheduler fails
     */
    void destroyExecutorInstances(List<String> ids) throws ISchedulerException;

    /**
     * Tests the scheduler connection
     *
     * @throws ISchedulerException Scheduler is down
     */
    void healthCheck() throws ISchedulerException;

    /**
     * Get scheduler name
     */
    String getName();

    /**
     * Check compatibility with dynamic scheduling
     */
    boolean isDynamic();
}
