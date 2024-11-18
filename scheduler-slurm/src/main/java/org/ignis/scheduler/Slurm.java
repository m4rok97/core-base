package org.ignis.scheduler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.ignis.scheduler3.model.IContainerInfo;
import org.ignis.scheduler3.IScheduler;
import org.ignis.scheduler3.ISchedulerException;
import org.ignis.scheduler3.model.IBindMount;
import org.ignis.scheduler3.model.IClusterInfo;
import org.ignis.scheduler3.model.IClusterRequest;
import org.ignis.scheduler3.model.IContainerInfo.IStatus;
import org.ignis.scheduler3.model.IJobInfo;
import org.slf4j.LoggerFactory;

public final class Slurm implements IScheduler {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Slurm.class);

    public static final String NAME = "slurm";
     
    private boolean executorContainers;
    private final String cmd;

    public Slurm(String url) {
        cmd = url;
        executorContainers = false;
    }

    private String esc(String v) {
        v = v.replace("\n", "\\n");
        if (v.contains(" ")) {
            return "\"" + v + "\"";
        }
        return v;
    }

    private void parseSingulauryContainerArgs(StringBuilder script, IContainerInfo containerInfo) throws ISchedulerException {
        script.append("singularity exec");
        script.append(" --writable-tmpfs --pid --cleanenv");

        for (IBindMount bindMount : containerInfo.binds()) {
            script.append(" --bind \"").append(bindMount.host()).append(":").append(bindMount.container());
            script.append(":").append(bindMount.ro() ? "ro" : "rw").append("\"");
        }

        script.append(" --bind $(mktemp -d):/ssh:rw");

        for (Map.Entry<String, String> entry : containerInfo.env().entrySet()) {
            script.append(" --env ").append(esc(entry.getKey() + "=" + entry.getValue()));
        }
        script.append(" --env IGNIS_JOB_ID=${IGNIS_JOB_ID}");
        script.append(" --env IGNIS_JOB_NAME=${IGNIS_JOB_NAME}");
        script.append(" --env SCHEDULER_PATH=${SCHEDULER_PATH}");

        script.append(' ').append(containerInfo.image()).append(' ').append(containerInfo.command());
        for (String arg : c.getArguments()) {
            script.append(' ').append(esc(arg));
        }
        script.append('\n');
    }

    private void parseContainerArgs(StringBuilder script, IContainerInfo containerInfo, String wd, boolean  driver) throws ISchedulerException {
        String port = containerInfo.schedulerOptArgs().get("port");
        if(port != null){
            int initPort = Integer.parseInt(port);
            int endPort = initPort + containerInfo.ports().size();

            script.append("export SLURM_STEP_RESV_PORTS=").append(initPort).append("-").append(endPort).append('\n');
        }

        for (IBindMount bindMount : containerInfo.binds()) {
            if (bindMount.host().equals(wd)) {
                script.append("export SCHEDULER_PATH='").append(bindMount.container()).append("'\n");
                break;
            }
        }
        
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(containerInfo);
            script.append("export CONTAINER_INFO=").append(new String(Base64.getEncoder().encode(bos.toByteArray()))).append("\n");
        } catch (IOException e) {
            throw new ISchedulerException("IO error", e);
        }

        script.append("export IGNIS_JOB_ID=").append(driver ? "driver" : "${SLURM_PROCID}").append("\n");
        script.append("export IGNIS_JOB_NAME=${SLURM_JOB_NAME}-${SLURM_JOB_ID}\n");

        String wd2 = wd.endsWith("/") ? wd : wd + "/";
        String file = wd2 + "${IGNIS_JOB_NAME}/slurm/${IGNIS_JOB_ID}";

        script.append("mkdir -p ").append(wd2).append("${IGNIS_JOB_NAME}/slurm\n");
        script.append("env --null > ").append(file).append(".env\n");
        script.append("echo 1 > ").append(file).append(".ok\n");
        script.append("{\n");

        parseSingulauryContainerArgs(script, containerInfo);

        script.append("} > ").append(file).append(".out 2> ").append(file).append(".err\n");
    }
    
    private void parseSlurmArgs(StringBuilder script, IContainerInfo containerInfo, int instances)
            throws ISchedulerException {
        
                script.append("#SBATCH --nodes=").append(instances).append('\n');
        script.append("#SBATCH --cpus-per-task=").append(containerInfo.cpus()).append('\n');
        script.append("#SBATCH --mem=").append(containerInfo.memory() / 1000 / 1000).append('\n');
        if (!Boolean.getBoolean("ignis.debug")) {
            script.append("#SBATCH --output=/dev/null").append('\n');
        }
    }

    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors)
            throws ISchedulerException {
        StringBuilder script = new StringBuilder();
        script.append("#!/bin/bash").append('\n');
        script.append("#SBATCH --job-name=").append(name).append('\n');
        
        parseSlurmArgs(script, driver.resources(), 1);
        
        List<String> args = driver.resources().args();
        if (!args.isEmpty()) {
            script.append("#SBATCH ").append(args).append('\n');
        }
        script.append("#SBATCH hetjob").append('\n');
        if (!args.isEmpty()) {
            script.append("#SBATCH ").append(args).append('\n');
        }
        for (IClusterRequest executor : executors) {
            parseSlurmArgs(script, executor.resources(), 1);
        }
        
        String errorCheck = "trap \"scancel --batch ${SLURM_JOBID}\" err\n";
        String exit = "trap \"exit 0\" SIGUSR1\n";

        script.append(exit).append("\n");
        script.append("DRIVER=$(cat - <<'EOF'").append("\n");
        script.append("#!/bin/bash\n");
        script.append("trap \"scancel --batch --signal=USR1 ${SLURM_JOBID}\" exit\n");
        script.append(exit);
        script.append(errorCheck);


    }

    @Override
    public void cancelJob(String id) throws ISchedulerException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'cancelJob'");
    }

    @Override
    public IJobInfo getJob(String id) throws ISchedulerException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getJob'");
    }

    @Override
    public List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'listJobs'");
    }

    @Override
    public IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createCluster'");
    }

    @Override
    public IClusterInfo getCluster(String job, String id) throws ISchedulerException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCluster'");
    }

    @Override
    public void destroyCluster(String job, String id) throws ISchedulerException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'destroyCluster'");
    }

    @Override
    public IClusterInfo repairCluster(String job, IClusterInfo cluster, IClusterRequest request)
            throws ISchedulerException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'repairCluster'");
    }

    @Override
    public IStatus getContainerStatus(String job, String id) throws ISchedulerException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getContainerStatus'");
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'healthCheck'");
    }
    
}
