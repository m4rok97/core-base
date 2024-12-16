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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    private void parseSingulauryContainerArgs(StringBuilder script, IContainerInfo containerInfo)
            throws ISchedulerException {
        // TODO: I need to update this part and use an implementation that the singularity implementation 
        script.append("ignis-host singularity exec");
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

        script.append(' ').append(containerInfo.image()).append(' ').append(containerInfo.args().get(0));
        for (String arg : c.getArguments()) {
            script.append(' ').append(esc(arg));
        }
        script.append('\n');
    }

    private void parseContainerArgs(StringBuilder script, IContainerInfo containerInfo, boolean  driver) throws ISchedulerException {
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


        String wd = "${IGNIS_WDIR}/";
        String file = wd + "${IGNIS_JOB_NAME}/slurm/${IGNIS_JOB_ID}";

        script.append("mkdir -p ").append(wd).append("${IGNIS_JOB_NAME}/slurm\n");
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


    private void run(List<String> args, String script) throws ISchedulerException {
        List<String> cmdArgs = new ArrayList<>();
        cmdArgs.add(cmd);
        cmdArgs.addAll(args);

        ProcessBuilder builder = new ProcessBuilder(cmdArgs);
        try {
            builder.inheritIO().redirectInput(ProcessBuilder.Redirect.PIPE);
            Process slurm = builder.start();
            slurm.getOutputStream().write(script.getBytes(StandardCharsets.UTF_8));
            slurm.getOutputStream().close();
            int exitCode = slurm.waitFor();
            if (exitCode != 0) {
                throw new ISchedulerException("slurm returns exit code != 0, (" + exitCode + ")");
            }
        } catch (IOException e) {
            throw new ISchedulerException("slurm error", e);
        } catch (InterruptedException ignored) {
        }
    }
    

    private String runAndCaptureOutput(List<String> args, String script) throws ISchedulerException {
        List<String> cmdArgs = new ArrayList<>();
        cmdArgs.add(cmd);
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
    ProcessBuilder builder = new ProcessBuilder(args); // Use args directly as the command
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
        // TODO: I need to review the get id method  
        StringBuilder script = new StringBuilder();
        script.append("#!/bin/bash").append('\n');
        script.append("#SBATCH --job-name=").append(name).append('\n');
        
        parseSlurmArgs(script, driver.resources(), 1);
        
        List<String> args = driver.resources().args();
        if (!args.isEmpty()) {
            script.append("#SBATCH ").append(args).append('\n');
        }
       
        for (IClusterRequest executor : executors) {
            script.append("#SBATCH hetjob").append('\n');
            if (!args.isEmpty()) {
                script.append("#SBATCH ").append(executor.resources().args()).append('\n');
            }
            parseSlurmArgs(script, executor.resources(), executor.instances());
        }
        
        String errorCheck = "trap \"scancel --batch ${SLURM_JOBID}\" err\n";
        String exit = "trap \"exit 0\" SIGUSR1\n";

        script.append(exit).append("\n");
        script.append("DRIVER=$(cat - <<'EOF'").append("\n");
        script.append("#!/bin/bash\n");
        script.append("trap \"scancel --batch --signal=USR1 ${SLURM_JOBID}\" exit\n");
        script.append(exit);
        script.append(errorCheck);
        parseContainerArgs(script, driver.resources(), true);
        script.append("EOF").append("\n");
        script.append(")").append("\n");
        script.append("\n");
        script.append("EXECUTOR=$(cat - <<'EOF'").append("\n");
        script.append("#!/bin/bash\n");
        script.append(exit);
        script.append(errorCheck);
        
        for (IClusterRequest executor : executors) {
            parseContainerArgs(script, executor.resources(), false);
            script.append("EOF").append("\n");
            script.append(")").append("\n");
        }
        
        String resvPorts = "";

        if (!driver.resources().ports().isEmpty()) {
            resvPorts = " --resv-ports=" + driver.resources().ports().size();
        }



        script.append("\n");
        script.append("srun").append(resvPorts).append(" --het-group=1 bash - <<< ${EXECUTOR} &").append("\n");
        script.append("srun").append(resvPorts).append(" --het-group=0 bash - <<< ${DRIVER}   &").append("\n");
        script.append("wait\n");

        if (Boolean.getBoolean("ignis.debug")) {
            LOGGER.info("Debug: slurm script{ \n    " + script.toString().replace("\n", "\n    ") + "\n}\n");
        }
       
        String output = runAndCaptureOutput(List.of(), script.toString());
        
        String jobId = extractJobId(output);
        if (jobId == null) {
            throw new ISchedulerException("Failed to retrieve the job ID from Slurm output");
        }
        
        return jobId;
    }

    @Override
    public void cancelJob(String id) throws ISchedulerException {
    // Validate the input job ID
    if (id == null || id.isEmpty()) {
        throw new ISchedulerException("Job ID cannot be null or empty");
    }

    List<String> cmdArgs = List.of("scancel", id);

    ProcessBuilder builder = new ProcessBuilder(cmdArgs);

    try {
        Process process = builder.start();

        int exitCode = process.waitFor();

        if (exitCode != 0) {
            String errorMessage = new BufferedReader(new InputStreamReader(process.getErrorStream()))
                    .lines()
                    .collect(Collectors.joining("\n"));
            throw new ISchedulerException("Failed to cancel job with ID " + id + ". Error: " + errorMessage);
        }

    } catch (IOException e) {
        throw new ISchedulerException("Error executing `scancel` command", e);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ISchedulerException("Job cancellation process was interrupted", e);
    }
}


    @Override
    public IJobInfo getJob(String id) throws ISchedulerException {
    if (id == null || id.isEmpty()) {
        throw new ISchedulerException("Job ID cannot be null or empty.");
    }

    List<String> cmdArgs = List.of("scontrol", "show", "job", id);
    ProcessBuilder builder = new ProcessBuilder(cmdArgs);

    try {
        Process process = builder.start();

        String output;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            output = reader.lines().collect(Collectors.joining("\n"));
        }

        int exitCode = process.waitFor();

        if (exitCode != 0) {
            String errorMessage = new BufferedReader(new InputStreamReader(process.getErrorStream()))
                    .lines()
                    .collect(Collectors.joining("\n"));
            throw new ISchedulerException("Failed to retrieve job info for ID " + id + ". Error: " + errorMessage);
        }

        // Parse the output and return an IJobInfo object
        return parseJobInfo(id, output);

    } catch (IOException e) {
        throw new ISchedulerException("Error executing `scontrol` command.", e);
    } catch (InterruptedException e) {
        // Restore the interrupt status of the thread
        Thread.currentThread().interrupt();
        throw new ISchedulerException("Job retrieval process was interrupted.", e);
    }
}



    private IJobInfo parseJobInfo(String jobId, String output) {
    Map<String, String> jobDetails = new HashMap<>();

    String[] lines = output.split("\n");
    for (String line : lines) {
        String[] tokens = line.trim().split("\\s+");
        for (String token : tokens) {
            String[] keyValue = token.split("=", 2);
            if (keyValue.length == 2) {
                jobDetails.put(keyValue[0], keyValue[1]);
            }
        }
    }

    String name = jobDetails.getOrDefault("JobName", "Unknown");

    List<IClusterInfo> clusters = new ArrayList<>();
    String nodes = jobDetails.get("Nodes");
    if (nodes != null) {
        String[] nodeList = nodes.split(",");
        for (String node : nodeList) {
            clusters.add(buildClusterInfo(node, jobDetails));
        }
    }

    // Return the constructed IJobInfo record
    return new IJobInfo(name, jobId, clusters);
}

    private IClusterInfo buildClusterInfo(String node, Map<String, String> jobDetails) {
    int instances = Integer.parseInt(jobDetails.getOrDefault("NumNodes", "1"));

    List<IContainerInfo> containers = new ArrayList<>();
    containers.add(buildContainerInfo(node, jobDetails));

    return new IClusterInfo(node, instances, containers);
}

    private IContainerInfo buildContainerInfo(String node, Map<String, String> jobDetails) {
    return IContainerInfo.builder()
            .id(jobDetails.getOrDefault("JobId", "Unknown"))
            .node(node)
            .image("Unknown")
            .args(List.of())
            .cpus(Integer.parseInt(jobDetails.getOrDefault("Cpus", "1")))
            .gpu(jobDetails.getOrDefault("Gres", ""))
            .memory(Long.parseLong(jobDetails.getOrDefault("Mem", "0")) * 1024L)
            .time(Long.parseLong(jobDetails.getOrDefault("TimeLimit", "0")))
            .user(jobDetails.getOrDefault("UserId", ""))
            .writable(false)
            .tmpdir(false)
            .ports(List.of())
            .binds(List.of())
            .nodelist(List.of(node))
            .hostnames(Map.of())
            .env(Map.of())
            .network(IContainerInfo.INetworkMode.HOST)
            .status(parseJobStatus(jobDetails.get("State")))
            .provider(IContainerInfo.IProvider.SINGULARITY)
            .schedulerOptArgs(Map.of())
            .build();
}

    private IContainerInfo.IStatus parseJobStatus(String state) {
    if (state == null) {
        return IContainerInfo.IStatus.UNKNOWN;
    }
    switch (state.toUpperCase()) {
        case "RUNNING":
            return IContainerInfo.IStatus.RUNNING;
        case "PENDING":
            return IContainerInfo.IStatus.ACCEPTED;
        case "COMPLETED":
            return IContainerInfo.IStatus.FINISHED;
        case "FAILED":
        case "CANCELLED":
            return IContainerInfo.IStatus.ERROR;
        default:
            return IContainerInfo.IStatus.UNKNOWN;
    }
}


    


   @Override
public List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException {
    List<String> cmdArgs = new ArrayList<>();
    cmdArgs.add("squeue");
    cmdArgs.add("--noheader");
    cmdArgs.add("--format=%i|%j|%u|%T|%N");

    ProcessBuilder builder = new ProcessBuilder(cmdArgs);
    try {
        Process process = builder.start();

        List<String> outputLines;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            outputLines = reader.lines().collect(Collectors.toList());
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            String errorMessage = new BufferedReader(new InputStreamReader(process.getErrorStream()))
                    .lines()
                    .collect(Collectors.joining("\n"));
            throw new ISchedulerException("Failed to list jobs. Error: " + errorMessage);
        }

        return parseJobs(outputLines, filters);

    } catch (IOException e) {
        throw new ISchedulerException("Error executing `squeue` command.", e);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ISchedulerException("Job listing process was interrupted.", e);
    }
}


private List<IJobInfo> parseJobs(List<String> outputLines, Map<String, String> filters) {
    List<IJobInfo> jobs = new ArrayList<>();

    for (String line : outputLines) {
        String[] attributes = line.split("\\|");
        if (attributes.length < 5) {
            continue;
        }

        String jobId = attributes[0];
        String jobName = attributes[1];
        String user = attributes[2];
        String state = attributes[3];
        String nodeList = attributes[4];

        if (filters != null) {
            if (filters.containsKey("jobName") && !jobName.equals(filters.get("jobName"))) {
                continue;
            }
            if (filters.containsKey("user") && !user.equals(filters.get("user"))) {
                continue;
            }
            if (filters.containsKey("state") && !state.equalsIgnoreCase(filters.get("state"))) {
                continue;
            }
        }

        IJobInfo job = new IJobInfo(
            jobName,
            jobId,
            List.of(new IClusterInfo(
                nodeList,
                1,
                List.of(new IContainerInfo(
                    jobId,
                    nodeList,
                    "Unknown",
                    List.of(),
                    1,
                    "",
                    0,
                    null,
                    user,
                    false,
                    false,
                    List.of(),
                    List.of(),
                    List.of(nodeList),
                    Map.of(),
                    Map.of(),
                    IContainerInfo.INetworkMode.HOST,
                    parseJobStatus(state),
                    IContainerInfo.IProvider.SINGULARITY,
                    Map.of()
                ))
            ))
        );

        jobs.add(job);
    }

    return jobs;
}

    @Override
    public IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException {
        StringBuilder script = new StringBuilder();

        script.append("#!/bin/bash").append('\n');
        script.append("#SBATCH --job-name=").append(job).append('\n');

        parseSlurmArgs(script, request.resources(), request.instances());

        List<String> args = request.resources().args();
        if (!args.isEmpty()) {
            script.append("#SBATCH ").append(String.join(" ", args)).append('\n');
        }

        script.append("CLUSTER=$(cat - <<'EOF'").append("\n");
        script.append("#!/bin/bash\n");

        script.append("trap \"scancel --batch ${SLURM_JOBID}\" ERR\n");
        script.append("trap \"exit 0\" SIGUSR1\n");

        parseContainerArgs(script, request.resources(), false);

        script.append("EOF\n");
        script.append(")\n");

        String resvPorts = "";
        if (!request.resources().ports().isEmpty()) {
            resvPorts = " --resv-ports=" + request.resources().ports().size();
        }
        script.append("srun").append(resvPorts).append(" bash - <<< ${CLUSTER} &\n");
        script.append("wait\n");

        if (Boolean.getBoolean("ignis.debug")) {
            LOGGER.info("Debug: slurm script{ \n    " + script.toString().replace("\n", "\n    ") + "\n}\n");
        }

        String output = runAndCaptureOutput(List.of(), script.toString());

        String clusterId = extractJobId(output);
        if (clusterId == null) {
            throw new ISchedulerException("Failed to retrieve the cluster ID from Slurm output");
        }

        return IClusterInfo.builder()
                .id(clusterId)
                .instances(request.instances())
                .containers(parseContainers(clusterId))
                .build();
    }

private List<IContainerInfo> parseContainers(String clusterId) throws ISchedulerException {
    try {
        String command = "scontrol show job " + clusterId;
        String output = runAndCaptureOutput(List.of("/bin/bash", "-c", command));

        List<IContainerInfo> containers = new ArrayList<>();

        String[] lines = output.split("\n");
        for (String line : lines) {
            if (line.contains("NodeList")) {
                String node = extractValue(line, "NodeList");
                String cpus = extractValue(line, "NumCPUs");
                String memory = extractValue(line, "MinMemoryNode");

                containers.add(IContainerInfo.builder()
                        .id(clusterId + "-container")
                        .node(node)
                        .image(null)
                        .args(List.of())
                        .cpus(Integer.parseInt(cpus))
                        .gpu(null)
                        .memory(Long.parseLong(memory) * 1024 * 1024)
                        .time(null)
                        .user(null)
                        .writable(false)
                        .tmpdir(false)
                        .ports(List.of())
                        .binds(List.of())
                        .nodelist(List.of(node))
                        .hostnames(Map.of())
                        .env(Map.of())
                        .network(IContainerInfo.INetworkMode.HOST)
                        .status(IContainerInfo.IStatus.RUNNING)
                        .provider(IContainerInfo.IProvider.SINGULARITY)
                        .schedulerOptArgs(Map.of())
                        .build());
            }
        }

        return containers;
    } catch (Exception ex) {
        throw new ISchedulerException("Failed to parse container information: " + ex.getMessage(), ex);
    }
}

private String extractValue(String line, String key) {
    int index = line.indexOf(key + "=");
    if (index == -1) {
        return null;
    }
    int start = index + key.length() + 1;
    int end = line.indexOf(' ', start);
    return end == -1 ? line.substring(start) : line.substring(start, end);
}


    @Override
    public IClusterInfo getCluster(String job, String id) throws ISchedulerException {
        try {
            String command = String.format("scontrol show jobid=%s", id);
            String output = runAndCaptureOutput(List.of("/bin/bash", "-c", command));

            IClusterInfo clusterInfo = parseClusterInfo(job, id, output);

            if (clusterInfo == null) {
                throw new ISchedulerException("Cluster not found for job ID: " + id);
            }

            return clusterInfo;
        } catch (Exception e) {
            throw new ISchedulerException("Error retrieving cluster information for job " + job + ", cluster " + id, e);
        }
    }


    private IClusterInfo parseClusterInfo(String job, String clusterId, String output) {
        // Parse the output from `scontrol show jobid` to extract cluster information
        int instances = 1; // Default to 1 instance if not explicitly mentioned
        List<IContainerInfo> containers = new ArrayList<>();

        String[] lines = output.split("\n");
        Map<String, String> jobAttributes = new HashMap<>();

        // Parse the output into a key-value map
        for (String line : lines) {
            String[] keyValue = line.split("=", 2);
            if (keyValue.length == 2) {
                jobAttributes.put(keyValue[0].trim(), keyValue[1].trim());
            }
        }

        // Extract relevant details
        String clusterName = jobAttributes.getOrDefault("JobName", "unknown");
        String nodes = jobAttributes.getOrDefault("Nodes", "");
        String state = jobAttributes.getOrDefault("JobState", "UNKNOWN");
        String user = jobAttributes.getOrDefault("UserId", "");
        long memory = parseMemory(jobAttributes.getOrDefault("MinMemoryNode", "0"));
        long time = parseTime(jobAttributes.getOrDefault("TimeLimit", "0-00:00:00")); // Slurm's format is days-hh:mm:ss
        int cpus = Integer.parseInt(jobAttributes.getOrDefault("NumCPUs", "1"));

        // Create container info (can be adjusted as per Slurm details)
        containers.add(IContainerInfo.builder()
                .id(clusterId)
                .node(nodes)
                .cpus(cpus)
                .memory(memory)
                .time(time)
                .user(user)
                .status(mapStatus(state))
                .build());

        return IClusterInfo.builder()
                .id(clusterId)
                .instances(instances)
                .containers(containers)
                .build();
    }

    private long parseMemory(String memoryStr) {
        if (memoryStr.endsWith("K")) {
            return Long.parseLong(memoryStr.replace("K", "")) * 1024;
        } else if (memoryStr.endsWith("M")) {
            return Long.parseLong(memoryStr.replace("M", "")) * 1024 * 1024;
        } else if (memoryStr.endsWith("G")) {
            return Long.parseLong(memoryStr.replace("G", "")) * 1024 * 1024 * 1024;
        }
        return Long.parseLong(memoryStr); // Assume bytes if no suffix
    }

    private long parseTime(String timeStr) {
        // Slurm time format: days-hh:mm:ss
        String[] parts = timeStr.split("[-:]");
        long days = 0, hours = 0, minutes = 0, seconds = 0;

        if (parts.length == 4) { // days-hh:mm:ss
            days = Long.parseLong(parts[0]);
            hours = Long.parseLong(parts[1]);
            minutes = Long.parseLong(parts[2]);
            seconds = Long.parseLong(parts[3]);
        } else if (parts.length == 3) { // hh:mm:ss
            hours = Long.parseLong(parts[0]);
            minutes = Long.parseLong(parts[1]);
            seconds = Long.parseLong(parts[2]);
        }

        return days * 86400 + hours * 3600 + minutes * 60 + seconds; // Convert to seconds
    }

    private IContainerInfo.IStatus mapStatus(String slurmState) {
    if (slurmState == null || slurmState.isBlank()) {
        return IContainerInfo.IStatus.UNKNOWN; // Handle null or empty state
    }

    // Map Slurm states to IContainerInfo.IStatus using a predefined mapping
    Map<String, IContainerInfo.IStatus> stateMap = Map.ofEntries(
        Map.entry("PENDING", IContainerInfo.IStatus.ACCEPTED),
        Map.entry("RUNNING", IContainerInfo.IStatus.RUNNING),
        Map.entry("COMPLETED", IContainerInfo.IStatus.FINISHED),
        Map.entry("COMPLETING", IContainerInfo.IStatus.FINISHED),
        Map.entry("FAILED", IContainerInfo.IStatus.ERROR),
        Map.entry("CANCELLED", IContainerInfo.IStatus.ERROR),
        Map.entry("TIMEOUT", IContainerInfo.IStatus.ERROR),
        Map.entry("NODE_FAIL", IContainerInfo.IStatus.ERROR),
        Map.entry("PREEMPTED", IContainerInfo.IStatus.ERROR),
        Map.entry("SUSPENDED", IContainerInfo.IStatus.UNKNOWN),
        Map.entry("UNKNOWN", IContainerInfo.IStatus.UNKNOWN)
    );

    // Normalize the input state to uppercase and retrieve its mapped status
    return stateMap.getOrDefault(slurmState.toUpperCase(), IContainerInfo.IStatus.UNKNOWN);
}



    @Override
public void destroyCluster(String job, String id) throws ISchedulerException {
    try {
        String command = String.format("scancel %s", id);
        runAndCaptureOutput(List.of("/bin/bash", "-c", command));

        LOGGER.info("Successfully destroyed cluster for job: {}, cluster ID: {}", job, id);
    } catch (Exception e) {
        throw new ISchedulerException("Error destroying cluster for job: " + job + ", cluster ID: " + id, e);
    }
}

   @Override
    public IClusterInfo repairCluster(String job, IClusterInfo cluster, IClusterRequest request)
            throws ISchedulerException {
        List<IContainerInfo> containers = cluster.containers();
        Map<String, IContainerInfo.IStatus> containerStatuses = new HashMap<>();

        for (IContainerInfo container : containers) {
            try {
                containerStatuses.put(container.id(), getContainerStatus(job, container.id()));
            } catch (Exception ex) {
                containerStatuses.put(container.id(), IContainerInfo.IStatus.UNKNOWN);
            }
        }

        List<IContainerInfo> newContainers = new ArrayList<>();
        for (IContainerInfo container : containers) {
            IContainerInfo.IStatus status = containerStatuses.getOrDefault(container.id(), IContainerInfo.IStatus.UNKNOWN);

            if (!IContainerInfo.IStatus.RUNNING.equals(status)) {
                try {
                    destroyCluster(job, container.id());
                } catch (Exception ex) {
                    LOGGER.warn("Failed to destroy container: " + container.id(), ex);
                }

                newContainers.add(parseRequest(job, request).get(0)); // Parse and add a new container
            } else {
                newContainers.add(container);
            }
        }

        List<IContainerInfo> startedContainers = startContainers(newContainers);

        return new IClusterInfo(cluster.id(), cluster.instances(), startedContainers);
    }


    private List<IContainerInfo> startContainers(List<IContainerInfo> containers) throws ISchedulerException {
    List<IContainerInfo> startedContainers = new ArrayList<>();

    for (IContainerInfo container : containers) {
        try {
            StringBuilder script = new StringBuilder();
            script.append("#!/bin/bash").append('\n');
            script.append("#SBATCH --job-name=").append(container.id()).append('\n');
            script.append("#SBATCH --nodes=1").append('\n');
            script.append("#SBATCH --cpus-per-task=").append(container.cpus()).append('\n');
            script.append("#SBATCH --mem=").append(container.memory()).append('\n');

            if (container.gpu() != null && !container.gpu().isEmpty()) {
                script.append("#SBATCH --gres=gpu:").append(container.gpu()).append('\n');
            }

            List<String> args = container.args();
            if (!args.isEmpty()) {
                script.append(String.join(" ", args)).append('\n');
            }

            script.append("srun ");
            if (container.network() == IContainerInfo.INetworkMode.HOST) {
                script.append("--network=host ");
            }

            script.append("singularity exec ")
                  .append("--bind /path/to/bind ")
                  .append(container.image())
                  .append(" ")
                  .append(String.join(" ", container.args()))
                  .append('\n');

            String output = runAndCaptureOutput(List.of("/bin/bash", "-c", script.toString()));

            String jobId = extractJobId(output);
            if (jobId == null) {
                throw new ISchedulerException("Failed to start container: " + container.id());
            }

            startedContainers.add(new IContainerInfo(
                jobId,
                container.node(),
                container.image(),
                container.args(),
                container.cpus(),
                container.gpu(),
                container.memory(),
                container.time(),
                container.user(),
                container.writable(),
                container.tmpdir(),
                container.ports(),
                container.binds(),
                container.nodelist(),
                container.hostnames(),
                container.env(),
                container.network(),
                IContainerInfo.IStatus.RUNNING,
                container.provider(),
                container.schedulerOptArgs()
            ));
        } catch (Exception ex) {
            LOGGER.error("Failed to start container: " + container.id(), ex);
            throw new ISchedulerException("Error starting container: " + container.id(), ex);
        }
    }

    return startedContainers;
}

private List<IContainerInfo> parseRequest(String job, IClusterRequest request) throws ISchedulerException {
    List<IContainerInfo> containers = new ArrayList<>();

    for (int i = 0; i < request.instances(); i++) {
        String containerId = job + "-instance-" + i;

        var resources = request.resources();

        IContainerInfo container = IContainerInfo.builder()
                .id(containerId)
                .node(request.resources().node())
                .image(resources.image())
                .args(resources.args())
                .cpus(resources.cpus())
                .gpu(resources.gpu())
                .memory(resources.memory())
                .time(resources.time())
                .user(resources.user())
                .writable(resources.writable())
                .tmpdir(resources.tmpdir())
                .ports(resources.ports()) 
                .binds(resources.binds()) 
                .nodelist(resources.nodelist())
                .hostnames(resources.hostnames())
                .env(resources.env())
                .network(resources.network())
                .status(IContainerInfo.IStatus.ACCEPTED)
                .provider(IContainerInfo.IProvider.SINGULARITY)
                .schedulerOptArgs(resources.schedulerOptArgs())
                .build();

        containers.add(container);
    }

    return containers;
}



    @Override
    public IContainerInfo.IStatus getContainerStatus(String job, String id) throws ISchedulerException {
        try {
            String command = String.format("squeue -j %s --noheader --format=%%T", id);
            String output = runAndCaptureOutput(List.of("/bin/bash", "-c", command)).trim();

            if (output.isEmpty()) {
                command = String.format("sacct -j %s --format=State --noheader", id);
                output = runAndCaptureOutput(List.of("/bin/bash", "-c", command)).trim();

                if (output.isEmpty()) {
                    return IContainerInfo.IStatus.UNKNOWN;
                }
            }

            return mapStatus(output.split("\n")[0].trim());

        } catch (Exception ex) {
            LOGGER.error("Failed to get container status for job " + id, ex);
            throw new ISchedulerException("Error getting container status for job: " + id, ex);
        }
    }




    @Override
    public void healthCheck() throws ISchedulerException {
        try {
            // Run a simple Slurm command to verify Slurm is responsive
            String command = "sinfo --noheader";
            runAndCaptureOutput(List.of("/bin/bash", "-c", command));
            LOGGER.info("Slurm health check passed.");
        } catch (Exception ex) {
            LOGGER.error("Slurm health check failed", ex);
            throw new ISchedulerException("Slurm health check failed: " + ex.getMessage(), ex);
        }
    }

    
}
