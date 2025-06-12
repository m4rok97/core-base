package org.ignis.scheduler;

import org.ignis.scheduler.model.IClusterInfo;
import org.ignis.scheduler.model.IClusterRequest;
import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IJobInfo;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author César Pomar
 * <p>
 * Scheduler parameters:
 * slurm.provider= : Allows to set a custom container provider
 */
public final class Slurm implements IScheduler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Slurm.class);

    private final String binary;

    public Slurm(String binary) {
        if (binary == null) {
            binary = "sbatch";
        }
        this.binary = binary;
    }

    private String provider(IContainerInfo info){
        return info.schedulerOptArgs().getOrDefault("slurm.provider", info.provider().name().toLowerCase());
    }

    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void cancelJob(String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IJobInfo getJob(String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IClusterInfo getCluster(String job, String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void destroyCluster(String job, String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IClusterInfo repairCluster(String job, IClusterInfo cluster, IClusterRequest request) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IContainerInfo.IStatus getContainerStatus(String job, String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
