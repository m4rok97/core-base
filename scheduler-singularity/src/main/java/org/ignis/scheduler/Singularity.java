package org.ignis.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.ignis.scheduler3.IScheduler;
import org.ignis.scheduler3.ISchedulerException;
import org.ignis.scheduler3.ISchedulerUtils;
import org.ignis.scheduler3.model.IClusterInfo;
import org.ignis.scheduler3.model.IClusterRequest;
import org.ignis.scheduler3.model.IContainerInfo;
import org.ignis.scheduler3.model.IJobInfo;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author César Pomar
 * <p>
 * Scheduler parameters:
 * singularity.cgroup=true : Allows to disable cgroup
 */
public class Singularity implements IScheduler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Singularity.class);

    private record CreateContainerCmd(String name, String cmd) {
    }

    private final String binary;

    public Singularity(String binary) {
        if (binary == null) {
            binary = "singularity";
        }
        this.binary = binary;
    }

    private JsonNode parseJson(String json) throws ISchedulerException {
        try {
            var mapper = new ObjectMapper();
            return mapper.readTree(json);
        } catch (IOException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    private String run(String... args) throws ISchedulerException {
        return run(Boolean.getBoolean("ignis.debug"), args);
    }

    private String run(boolean inheritIO, String... args) throws ISchedulerException {
        try {
            var processBuilder = new ProcessBuilder(args);
            if (inheritIO) {
                processBuilder.inheritIO();
            }
            var singularity = processBuilder.start();
            int code = singularity.waitFor();
            if (code != 0) {
                throw new ISchedulerException("code " + code + " != 0\n" + new String(singularity.getErrorStream().readAllBytes()));
            }
            if (!inheritIO) {
                return new String(singularity.getInputStream().readAllBytes());
            }
            return null;
        } catch (IOException | InterruptedException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    private List<String> getInstances() throws ISchedulerException {
        var list = parseJson(run("ignis-host", this.binary, "instance", "list", "--json"));
        var result = new ArrayList<String>();

        for (var instance : list.get("instances")) {
            result.add(instance.get("instance").asText());
        }
        return result;
    }

    private List<CreateContainerCmd> parseRequest(String jobID, IClusterRequest request) throws ISchedulerException {
        return parseRequest(jobID, request, false);
    }

    private String createPipes(String name, String cmd) {
        return """
                {cmd}
                code=$?
                if [ $code -ne 0 ]; then
                    exit $code
                fi
                nohup bash -c '
                    run_pipes(){
                        temp_dir=$(mktemp -d)
                        trap "rm -rf $temp_dir" EXIT

                        mkfifo "$temp_dir/run"
                        mkfifo "$temp_dir/code"
                        mkfifo "$temp_dir/out"
                        mkfifo "$temp_dir/err"

                        while true; do
                          read run < "$temp_dir/run"
                          if [ "$run" == "run\\n" ]; then
                            echo -n $(bash "$temp_dir/script" > "$temp_dir/out" 2> "$temp_dir/err") > "$temp_dir/code"
                          fi
                        done
                    }
                
                    {singularity} exec instance://{name} sleep inf &
                    RUNNING=$!
                    run_pipes &
                    wait $RUNNING
                    exit
                ' > /dev/null 2>&1 &

                exit $code
                """.replace("{name}", name).
                replace("{cmd}", cmd).
                replace("{singularity}", this.binary);
    }

    private List<CreateContainerCmd> parseRequest(String jobID, IClusterRequest request, boolean pipes) throws ISchedulerException {
        var resources = request.resources();
        var cmd = new ArrayList<>(Arrays.asList(this.binary, "instance", "start"));
        var cgroup = Boolean.parseBoolean(resources.schedulerOptArgs().getOrDefault("singularity.cgroup", "true"));

        if (cgroup) {
            cmd.add("--cpus");
            cmd.add(String.valueOf(resources.cpus()));
            cmd.add("--memory");
            cmd.add(String.valueOf(resources.memory()));
        }
        if (resources.gpu() != null) {
            cmd.add("--nv");
        }
        //resources.user()
        if (resources.writable() || resources.tmpdir()) {
            cmd.add("--writable-tmfs");
            if (resources.tmpdir()) {
                cmd.add("--env");
                cmd.add("IGNIS_TMPDIR=/tmp");
            }

        }
        if (resources.network().equals(IContainerInfo.INetworkMode.BRIDGE)) {
            cmd.add("--net");
            cmd.add("--network");
            cmd.add("bridge");
            for (var port : resources.ports()) {
                if (port.container() > 0) {
                    cmd.add("--network-args");
                    cmd.add("portmap=" + port.host() + ":" + port.container() + "/" + (port.protocol().toString().toLowerCase()));
                }
            }
        }
        if (resources.binds() != null) {
            for (var bind : resources.binds()) {
                cmd.add("--bind");
                cmd.add("\"" + bind.host() + ":" + bind.container() + ":" + (bind.ro() ? "ro" : "rw" + "\""));
            }
        }
        //resources.hostnames()
        cmd.add("--cleanenv");
        if (resources.env() != null) {
            for (var entry : resources.env().entrySet()) {
                cmd.add("--env");
                cmd.add("\"" + entry.getKey() + "=" + entry.getValue() + "\"");
            }
        }

        var createContainers = new ArrayList<CreateContainerCmd>();
        for (int i = 0; i < request.instances(); i++) {
            var instance = new ArrayList<>(cmd);
            var containerName = jobID + "-" + ISchedulerUtils.name(request.name()) + "-" + i;

            instance.add("--env");
            instance.add("SCHEDULER_RESOURCES=" + ISchedulerUtils.encode(resources));
            instance.add("--env");
            instance.add("SCHEDULER_INSTANCES=" + request.instances());
            instance.add("--env");
            instance.add("SCHEDULER_CLUSTER=" + request.name());
            instance.add("--env");
            instance.add("IGNIS_SCHEDULER_ENV_JOB=" + jobID);
            instance.add("--env");
            instance.add("IGNIS_SCHEDULER_ENV_CONTAINER=" + containerName);

            instance.add(resources.image());
            instance.add(containerName);
            instance.add("ignis-logger");
            instance.addAll(resources.args());

            String script = String.join(" ", instance);

            if (pipes) {
                script = createPipes(containerName, script);
            }

            createContainers.add(new CreateContainerCmd(containerName, script));
        }

        return createContainers;
    }

    private void startContainers(List<CreateContainerCmd> containers) throws ISchedulerException {
        var started = new ArrayList<String>();
        try {
            for (var instance : containers) {
                run("ignis-host", instance.cmd);
                started.add(instance.name);
            }
        } catch (ISchedulerException ex) {
            for (var c : started) {
                try {
                    run(false, "ignis-host", this.binary, "instance", "stop", c);
                } catch (ISchedulerException ex2) {
                }
            }
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    private void destroyContainers(List<String> containers) throws ISchedulerException {
        Exception error = null;
        for (var c : containers) {
            try {
                run("ignis-host", this.binary, "instance", "stop", c);
            } catch (Exception ex) {
                LOGGER.error(ex.getMessage());
                error = ex;
            }
        }
        if (error != null) {
            throw new ISchedulerException(error.getMessage(), error);
        }
    }

    private record RawContainer(String name, String job, String cluster, int instances, IContainerInfo resources) {
    }

    private RawContainer parseContainer(String name) throws ISchedulerException {
        String out = run("ignis-host", this.binary, "exec", "instance://" + name, "env", "--null");
        var env = new HashMap<String, String>();
        for (var entry : out.split("\0")) {
            var key_val = entry.split("=", 1);
            env.put(key_val[0], key_val[1]);
        }

        return new RawContainer(
                env.get("IGNIS_SCHEDULER_ENV_CONTAINER"),
                env.get("IGNIS_SCHEDULER_ENV_JOB"),
                env.get("SCHEDULER_CLUSTER"),
                Integer.parseInt(env.get("SCHEDULER_INSTANCES")),
                ISchedulerUtils.decode(env.get("SCHEDULER_RESOURCES"))
        );
    }

    private List<RawContainer> mapContainers(List<String> names) throws ISchedulerException {
        var result = new ArrayList<RawContainer>();
        for (var name : names) {
            result.add(parseContainer(name));
        }
        return result;
    }

    private List<IJobInfo> parseJobs(List<RawContainer> containers) {
        var jobs = new HashMap<String, IJobInfo>();

        NEXT:
        for (var container : containers) {
            if (jobs.containsKey(container.job)) {
                var job = jobs.get(container.job);
                for (var clusters : job.clusters()) {
                    if (clusters.id().equals(container.cluster)) {
                        clusters.containers().add(container.resources);
                        continue NEXT;
                    }
                }
                job.clusters().add(IClusterInfo.builder().
                        id(container.cluster).
                        instances(container.instances).
                        containers(new ArrayList<>(Collections.singletonList(container.resources))).build()
                );
            } else {
                jobs.put(container.job, IJobInfo.builder().
                        id(container.job).
                        name(container.job.split("-")[0]).clusters(new ArrayList<>()).build()
                );
            }
        }

        return new ArrayList<>(jobs.values());
    }

    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors) throws ISchedulerException {
        var id = ISchedulerUtils.genId();
        int idSz = Integer.parseInt(driver.resources().schedulerOptArgs().getOrDefault("docker.id", "5"));
        id = id.substring(0, Math.max(0, Math.min(idSz, id.length())));

        String jobID = ISchedulerUtils.name(name) + "-" + id;
        var containers = new ArrayList<>(parseRequest(jobID, driver, true));

        for (var exec : executors) {
            containers.addAll(parseRequest(jobID, exec));
        }
        startContainers(containers);

        return jobID;
    }

    @Override
    public void cancelJob(String id) throws ISchedulerException {
        var containers = getInstances();
        containers.removeIf(name -> !name.startsWith(id));
        destroyContainers(containers);
    }

    @Override
    public IJobInfo getJob(String id) throws ISchedulerException {
        var containers = getInstances();
        containers.removeIf(name -> !name.startsWith(id));
        if (containers.isEmpty()) {
            throw new ISchedulerException("job " + id + " not found");
        }
        return parseJobs(mapContainers(containers)).getFirst();
    }

    @Override
    public List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException {
        var containers = getInstances();
        return parseJobs(mapContainers(containers));
    }

    @Override
    public IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException {
        var containers = new ArrayList<>(parseRequest(job, request));
        startContainers(containers);
        return getCluster(job, containers.getFirst().name);
    }

    @Override
    public IClusterInfo getCluster(String job, String id) throws ISchedulerException {
        var containers = getInstances();
        containers.removeIf(name -> !name.startsWith(job + "-" + id));
        if (containers.isEmpty()) {
            throw new ISchedulerException("cluster " + id + " not found");
        }
        return parseJobs(mapContainers(containers)).getFirst().clusters().getFirst();
    }

    @Override
    public void destroyCluster(String job, String id) throws ISchedulerException {
        var containers = getInstances();
        containers.removeIf(name -> !name.startsWith(job + "-" + id));
        destroyContainers(containers);
    }

    @Override
    public IClusterInfo repairCluster(String job, IClusterInfo cluster, IClusterRequest request) throws ISchedulerException {
        var newContainers = new ArrayList<>(parseRequest(job, request));
        for (int i = 0; i < newContainers.size(); i++) {
            if (getContainerStatus(job, cluster.containers().get(i).id()).equals(IContainerInfo.IStatus.RUNNING)) {
                newContainers.set(i, null);
            }
        }
        newContainers.removeIf(Objects::isNull);
        startContainers(newContainers);
        return cluster;
    }

    @Override
    public IContainerInfo.IStatus getContainerStatus(String job, String id) throws ISchedulerException {
        var containers = getInstances();
        containers.removeIf(name -> !name.startsWith(job + "-" + id));
        if (containers.isEmpty()) {
            return IContainerInfo.IStatus.DESTROYED;
        }
        return IContainerInfo.IStatus.RUNNING;
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        run("ignis-host", this.binary, "version");
    }
}
