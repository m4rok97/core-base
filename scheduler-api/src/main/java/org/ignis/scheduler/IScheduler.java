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

import org.ignis.scheduler.model.IClusterInfo;
import org.ignis.scheduler.model.IClusterRequest;
import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IJobInfo;

import java.util.List;
import java.util.Map;

/**
 * @author César Pomar
 * <p>
 * It represents a common interface for IgnisHPC schedulers.<p>
 * Rules:<p>
 * 1) {@link IContainerInfo} must have at least the same information as IClusterRequest and must preserve the inner
 * order.<p>
 * 2) The scheduler must define 'IGNIS_SCHEDULER_ENV_JOB' and 'IGNIS_SCHEDULER_ENV_CONTAINER' environment variables
 *   with the return of {@link IScheduler#createJob} and the value of {@link IContainerInfo#id() IContainerInfo.id}.
 *   '$' can be used to reference a future container enviroment variable.<p>
 * 3) If network is bridge and container hostname cannot be resolved externally, 'HOST_HOSTNAME' must be set to a
 *   valid hostname.<p>
 * 4) If scheduler has its own healthcheck system, define 'IGNIS_HEALTHCHECK_DISABLE' to disable IgnisHPC healthcheck.
 */
public interface IScheduler {

    /**
     * Creates a new job with the specified name.
     *
     * @param name      The name of the job.
     * @param driver    The resources of the driver.
     * @param executors The resources of executors clusters.
     * @return A String ID representing the created job.
     * @throws ISchedulerException If an error occurs during job creation.
     */
    String createJob(String name, IClusterRequest driver, IClusterRequest... executors) throws ISchedulerException;

    /**
     * Cancels the job with the given ID.
     *
     * @param id The ID of the job to be canceled.
     * @throws ISchedulerException If an error occurs during job cancelation.
     */
    void cancelJob(String id) throws ISchedulerException;

    /**
     * Retrieves information about the job with the specified ID.
     *
     * @param id The ID of the job to retrieve information for.
     * @return An IJobInfo object containing information about the job.
     * @throws ISchedulerException If an error occurs during job retrieval.
     */
    IJobInfo getJob(String id) throws ISchedulerException;

    /**
     * Lists jobs based on the provided filters.
     *
     * @param filters A Map containing key-value pairs for filtering jobs.
     * @return A List of IJobInfo.
     * @throws ISchedulerException If an error occurs during job listing.
     */
    List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException;

    /**
     * Creates a new cluster with the given name. (Blocking function)
     *
     * @param job       The ID of the job to create a cluster.
     * @param request The resources of the cluster to be created.
     * @return An IClusterInfo object representing the created cluster.
     * @throws ISchedulerException If an error occurs during cluster creation.
     */
    IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException;

    /**
     * Gets the cluster with the specified ID.
     *
     * @param job The ID of the job.
     * @param id  The ID of the cluster to be retrieved.
     * @return An IClusterInfo object representing the cluster.
     * @throws ISchedulerException If an error occurs during cluster retrieval.
     */
    IClusterInfo getCluster(String job, String id) throws ISchedulerException;

    /**
     * Destroys the cluster with the specified ID.
     *
     * @param job The job ID of the job to create a cluster.
     * @param id  The ID of the cluster.
     * @throws ISchedulerException If an error occurs during cluster destruction.
     */
    void destroyCluster(String job, String id) throws ISchedulerException;

    /**
     * Repairs the specified cluster.
     *
     * @param job The ID of the job.
     * @param cluster An IClusterInfo object representing the cluster to be repaired.
     * @param request The resources of the cluster to be repaired.
     * @return An IClusterInfo object representing the repaired cluster.
     * @throws ISchedulerException If an error occurs during cluster repair.
     */
    IClusterInfo repairCluster(String job, IClusterInfo cluster, IClusterRequest request) throws ISchedulerException;

    /**
     * Gets the Status of container with the specified ID.
     *
     * @param job The ID of the job.
     * @param id  The ID of the container to retrieve the status.
     * @return An IContainerInfo.IStatus object representing the Status of container.
     * @throws ISchedulerException If an error occurs during container status retrieval.
     */
    IContainerInfo.IStatus getContainerStatus(String job, String id) throws ISchedulerException;

    /**
     * Performs a health check on the scheduler.
     *
     * @throws ISchedulerException If an error occurs during the health check.
     */
    void healthCheck() throws ISchedulerException;
}