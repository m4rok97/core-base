package org.ignis.scheduler;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ignis.scheduler.model.IBindMount;
import org.ignis.scheduler.model.IClusterInfo;
import org.ignis.scheduler.model.IClusterRequest;
import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IJobInfo;
import org.slf4j.LoggerFactory;

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

    private void parseSlurmArgs(StringBuilder script, IContainerInfo containerInfo, int instances)
            throws ISchedulerException {

        script.append("#SBATCH --nodes=").append(instances).append('\n');
        script.append("#SBATCH --cpus-per-task=").append(containerInfo.cpus()).append('\n');
        script.append("#SBATCH --mem=").append(containerInfo.memory() / 1000 / 1000).append('\n');
        if (!Boolean.getBoolean("ignis.debug")) {
            script.append("#SBATCH --output=/dev/null").append('\n');
        }
    }

    private String esc(String v) {
        v = v.replace("\n", "\\n");
        if (v.contains(" ")) {
            return "\"" + v + "\"";
        }
        return v;
    }

    private void parseSingularityContainerArgs(StringBuilder script, IContainerInfo containerInfo)
            throws ISchedulerException {
        // TODO: I need to update this part and use an implementation that the singularity implementation 
        // Add singularity exec command with the appropriate options
        script.append("ignis-host ").append(provider(containerInfo)).append(" singularity exec");
        script.append(" --writable-tmpfs --pid --cleanenv");
        
        // Add bind mounts
        for (IBindMount bindMount : containerInfo.binds()) {
            script.append(" --bind \"").append(bindMount.host()).append(":").append(bindMount.container());
            script.append(":").append(bindMount.ro() ? "ro" : "rw").append("\"");
        }

        // Add the SSH directory bind mount
        script.append(" --bind $(mktemp -d):/ssh:rw");

        // Add environment variables
        for (Map.Entry<String, String> entry : containerInfo.env().entrySet()) {
            script.append(" --env ").append(esc(entry.getKey() + "=" + entry.getValue()));
        }
        script.append(" --env IGNIS_JOB_ID=${IGNIS_JOB_ID}");
        script.append(" --env IGNIS_JOB_NAME=${IGNIS_JOB_NAME}");
        script.append(" --env SCHEDULER_PATH=${SCHEDULER_PATH}");

        script.append(' ').append(containerInfo.image()).append(' ').append(containerInfo.args().get(0));
        for (String arg : containerInfo.args()) {
            script.append(' ').append(esc(arg));
        }
        script.append('\n');
    }

    
    private void parseContainerArgs(StringBuilder script, IContainerInfo containerInfo, boolean driver)
            throws ISchedulerException {
        // Add port information if available
        String port = containerInfo.schedulerOptArgs().get("port");
        if(port != null){
            int initPort = Integer.parseInt(port);
            int endPort = initPort + containerInfo.ports().size();

            script.append("export SLURM_STEP_RESV_PORTS=").append(initPort).append("-").append(endPort).append('\n');
        }
        
        // Use IGNIS_WDIR as the working directory and export export SCHEDULER_PATH for each bind mount
        String wd = "${IGNIS_WDIR}";
        for (IBindMount bindMount : containerInfo.binds()) {
            if (bindMount.host().equals(wd)) {
                script.append("export SCHEDULER_PATH='").append(bindMount.container()).append("'\n");
                break;
            }
        }
        
        // Export container information
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(containerInfo);
            script.append("export CONTAINER_INFO=").append(new String(Base64.getEncoder().encode(bos.toByteArray()))).append("\n");
        } catch (IOException e) {
            throw new ISchedulerException("IO error", e);
        }

        // export the job ID and name
        script.append("export IGNIS_JOB_ID=").append(driver ? "driver" : "${SLURM_PROCID}").append("\n");
        script.append("export IGNIS_JOB_NAME=${SLURM_JOB_NAME}-${SLURM_JOB_ID}\n");


        // Create the working directory and environment files
        String file = wd + "/" + "${IGNIS_JOB_NAME}/slurm/${IGNIS_JOB_ID}";

        script.append("mkdir -p ").append(wd).append("${IGNIS_JOB_NAME}/slurm\n");
        script.append("env --null > ").append(file).append(".env\n");
        script.append("echo 1 > ").append(file).append(".ok\n");
        script.append("{\n");

        // Parse the singularity container arguments
        parseSingularityContainerArgs(script, containerInfo);

        script.append("} > ").append(file).append(".out 2> ").append(file).append(".err\n");
    }

    private String runAndCaptureOutput(List<String> args, String script) throws ISchedulerException {
        List<String> cmdArgs = new ArrayList<>();
        cmdArgs.add(binary);
        cmdArgs.addAll(args);

        ProcessBuilder builder = new ProcessBuilder(cmdArgs);
        StringBuilder output = new StringBuilder();

        try {
            builder.redirectErrorStream(true);
            Process slurm = builder.start();

            try (OutputStream os = slurm.getOutputStream()) {
                os.write(script.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(slurm.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            int exitCode = slurm.waitFor();
            if (exitCode != 0) {
                throw new ISchedulerException("Slurm returned exit code != 0, (" + exitCode + ")");
            }
        } catch (IOException | InterruptedException e) {
            throw new ISchedulerException("Error running Slurm command", e);
        }

        return output.toString();
    }

    private String runAndCaptureOutput(List<String> args) throws ISchedulerException {
        ProcessBuilder builder = new ProcessBuilder(args); 
        StringBuilder output = new StringBuilder();

        try {
            builder.redirectErrorStream(true);
            Process process = builder.start();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new ISchedulerException("Command returned exit code != 0, (" + exitCode + ")");
            }
        } catch (IOException | InterruptedException e) {
            throw new ISchedulerException("Error running command", e);
        }

        return output.toString();
    }

    private String extractJobId(String output) {
        Pattern pattern = Pattern.compile("Submitted batch job (\\d+)");
        Matcher matcher = pattern.matcher(output);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors)
            throws ISchedulerException {
        
        // Initialize the script with the shebang and job name
        StringBuilder script = new StringBuilder();
        script.append("#!/bin/bash").append('\n');
        script.append("#SBATCH --job-name=").append(name).append('\n');
        
        //  Parse slurm arguments for the driver
        parseSlurmArgs(script, driver.resources(), 1);

        // Add args from the driver
        List<String> args = driver.resources().args();
        if (!args.isEmpty()) {
            script.append("#SBATCH ").append(args).append('\n');
        }
        

        // Add the batch script header for executors
        for (IClusterRequest executor : executors) {
            script.append("#SBATCH hetjob").append('\n');
            if (!args.isEmpty()) {
                script.append("#SBATCH ").append(executor.resources().args()).append('\n');
            }
            parseSlurmArgs(script, executor.resources(), executor.instances());
        }

        String errorCheck = "trap \"scancel --batch ${SLURM_JOBID}\" err\n";
        String exit = "trap \"exit 0\" SIGUSR1\n";
        
        // Add the error check and exit trap
        script.append(exit).append("\n");
        script.append("DRIVER=$(cat - <<'EOF'").append("\n");
        script.append("#!/bin/bash\n");
        script.append("trap \"scancel --batch --signal=USR1 ${SLURM_JOBID}\" exit\n");
        script.append(exit);
        script.append(errorCheck);

        // Parse the driver container arguments
        parseContainerArgs(script, driver.resources(), true);
        script.append("EOF").append("\n");
        script.append(")").append("\n");
        script.append("\n");
        script.append("EXECUTOR=$(cat - <<'EOF'").append("\n");
        script.append("#!/bin/bash\n");
        script.append(exit);
        script.append(errorCheck);
        
        // Parse the executor container arguments
        for (IClusterRequest executor : executors) {
            parseContainerArgs(script, executor.resources(), false);
            script.append("EOF").append("\n");
            script.append(")").append("\n");
        }


        // Add resource reservation ports if available
        String resvPorts = "";
        if (!driver.resources().ports().isEmpty()) {
            resvPorts = " --resv-ports=" + driver.resources().ports().size();
        }
        // Add the srun commands to run the driver and executors
        script.append("\n");
        script.append("srun").append(resvPorts).append(" --het-group=1 bash - <<< ${EXECUTOR} &").append("\n");
        script.append("srun").append(resvPorts).append(" --het-group=0 bash - <<< ${DRIVER}   &").append("\n");
        script.append("wait\n");
        
        // Log the script if debugging is enabled
        if (Boolean.getBoolean("ignis.debug")) {
            LOGGER.info("Debug: slurm script{ \n    " + script.toString().replace("\n", "\n    ") + "\n}\n");
        }
       
        // Run the script and capture the output
        String output = runAndCaptureOutput(List.of(), script.toString());

        String jobId = extractJobId(output);
        if (jobId == null) {
            throw new ISchedulerException("Failed to retrieve the job ID from Slurm output");
        }
        
        return jobId;
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
