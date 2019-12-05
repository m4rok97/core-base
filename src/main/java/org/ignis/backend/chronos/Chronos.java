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
package org.ignis.backend.chronos;

import java.util.List;
import org.ignis.backend.chronos.model.Job;
import retrofit2.http.Body;
import retrofit2.http.DELETE;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.POST;
import retrofit2.http.PUT;
import retrofit2.http.Path;
import retrofit2.http.Query;

/**
 *
 * @author César Pomar
 */
public interface Chronos {

    @GET("scheduler/leader")
    public String leader() throws ChronosException;

    @GET("scheduler/jobs")
    public List<Job> jobs() throws ChronosException;

    @GET("scheduler/jobs/search?{attribute}={filter}")
    public List<Job> searchJobs(@Path("attribute") String attribute, @Path("filter") String filter) throws ChronosException;

    @DELETE("scheduler/job/{jobName}")
    public void deleteJob(@Path("jobName") String jobName) throws ChronosException;

    @DELETE("scheduler/task/kill/{jobName}")
    public void deleteJobTasks(@Path("jobName") String jobName) throws ChronosException;

    @PUT("scheduler/job/{jobName}")
    public void startJob(@Path("jobName") String name, @Query("arguments") String... arguments) throws ChronosException;

    @PUT("scheduler/job/success/{jobName}")
    public void makeJobSuccess(@Path("jobName") String name);

    public default void createJob(Job job) throws ChronosException {
        if (job.getSchedule() == null) {
            createDependentJob(job);
        } else {
            createSchedulerJob(job);
        }
    }

    @Headers("Content-Type: application/json")
    @POST("scheduler/iso8601")
    void createSchedulerJob(@Body Job job) throws ChronosException;

    @Headers("Content-Type: application/json")
    @POST("scheduler/dependency")
    void createDependentJob(@Body Job job) throws ChronosException;

    @POST("scheduler/job/{jobName}/task/{taskId}/progress")
    public void taskProgress(@Path("jobName") String jobName, @Path("taskId") String taskId, @Body int numAdditionalElementsProcessed);

    @GET("scheduler/graph/dot")
    public String graph();

}
