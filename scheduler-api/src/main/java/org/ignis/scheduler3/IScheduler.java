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
package org.ignis.scheduler3;

import org.ignis.scheduler3.model.IClusterInfo;
import org.ignis.scheduler3.model.IClusterRequest;
import org.ignis.scheduler3.model.IContainerInfo;
import org.ignis.scheduler3.model.IJobInfo;

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
 *   '$' can be used to reference a future container enviroment variable.
 */
public interface IScheduler {

    /**
     * Creates a new job with the specified name.
     *
     * @param name      The name of the job to be created.
     * @param driver    The resources of the driver of the job.
     * @param executors The resources of all clusters of the job.
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
     * @return A List of IJobInfo objects representing the filtered jobs.
     * @throws ISchedulerException If an error occurs during job listing.
     */
    List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException;

    /**
     * Creates a new cluster with the given name.
     *
     * @param job       The job ID of the job to create a cluster.
     * @param resources The resources of the cluster to be created.
     * @return An IClusterInfo object representing the created cluster.
     * @throws ISchedulerException If an error occurs during cluster creation.
     */
    IClusterInfo createCluster(String job, IClusterRequest resources) throws ISchedulerException;

    /**
     * Destroys the cluster with the specified ID.
     *
     * @param job The job ID of the job to create a cluster.
     * @param id  The ID of the cluster to be destroyed.
     * @throws ISchedulerException If an error occurs during cluster destruction.
     */
    void destroyCluster(String job, String id) throws ISchedulerException;

    /**
     * Gets the cluster with the specified ID.
     *
     * @param job The job ID of the job to get the cluster.
     * @param id  The ID of the cluster to be retrieved.
     * @return An IClusterInfo object representing the cluster.
     * @throws ISchedulerException If an error occurs during cluster retrieval.
     */
    IClusterInfo getCluster(String job, String id) throws ISchedulerException;

    /**
     * Gets the Status of cluster with the specified ID.
     *
     * @param job The job ID of the job to get the cluster status.
     * @param id  The ID of the cluster to retrieve the status.
     * @return An IContainerInfo.IStatus object representing the Status of cluster.
     * @throws ISchedulerException If an error occurs during cluster status retrieval.
     */
    IContainerInfo.IStatus getClusterStatus(String job, String id) throws ISchedulerException;

    /**
     * Repairs the specified cluster.
     *
     * @param job The job ID of the job to get the cluster status.
     * @param cluster An IClusterInfo object representing the cluster to be repaired.
     * @return An IClusterInfo object representing the repaired cluster.
     * @throws ISchedulerException If an error occurs during cluster repair.
     */
    IClusterInfo repairCluster(String job, IClusterInfo cluster) throws ISchedulerException;

    /**
     * Performs a health check on the scheduler.
     *
     * @throws ISchedulerException If an error occurs during the health check.
     */
    void healthCheck() throws ISchedulerException;
}