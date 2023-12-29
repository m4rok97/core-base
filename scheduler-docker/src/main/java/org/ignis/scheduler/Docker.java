package org.ignis.scheduler;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.exception.ConflictException;
import com.github.dockerjava.api.exception.DockerException;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import org.ignis.scheduler3.IScheduler;
import org.ignis.scheduler3.ISchedulerException;
import org.ignis.scheduler3.ISchedulerUtils;
import org.ignis.scheduler3.model.*;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author César Pomar
 * <p>
 * Scheduler parameters:
 * docker.gpu.driver=nvidia : Default driver for gpu request
 */
public class Docker implements IScheduler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Docker.class);
    private final DockerClient client;

    private final static Map<String, IContainerInfo.IStatus> DOCKER_STATUS = new HashMap<>() {
        {
            put("created", IContainerInfo.IStatus.ACCEPTED);
            put("restarting", IContainerInfo.IStatus.ACCEPTED);
            put("running", IContainerInfo.IStatus.RUNNING);
            put("exited", IContainerInfo.IStatus.FINISHED);
            put("removing", IContainerInfo.IStatus.DESTROYED);
            put("paused", IContainerInfo.IStatus.ERROR);
            put("dead", IContainerInfo.IStatus.ERROR);
        }
    };

    public Docker(String url) {
        if (url == null) {
            url = "/var/run/docker.sock";
        }
        if (!url.contains(":")) {
            url = "unix://" + url;
        }

        var config = DefaultDockerClientConfig.
                createDefaultConfigBuilder().
                withDockerHost(url).
                build();

        var httpClient = new ApacheDockerHttpClient.Builder().
                dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .connectionTimeout(Duration.ofSeconds(30))
                .responseTimeout(Duration.ofSeconds(45))
                .build();

        client = DockerClientImpl.getInstance(config, httpClient);
    }


    private List<CreateContainerCmd> parseRequest(String jobID, IClusterRequest request) throws ISchedulerException {
        var containers = new ArrayList<CreateContainerCmd>();
        for (int i = 0; i < request.instances(); i++) {
            var resources = request.resources();
            var container = client.createContainerCmd(resources.image());
            var env = new ArrayList<String>();
            container.withName(jobID + "-" + ISchedulerUtils.name(request.name()) + "-" + i);
            container.withHostConfig(new HostConfig());
            container.getHostConfig().withCpuCount((long) resources.cpus());
            if (resources.gpu() != null) {
                String defDriver = resources.schedulerOptArgs().getOrDefault("docker.gpu.driver", "nvidia");
                container.getHostConfig().withDeviceRequests(new ArrayList<>());
                String driver, count;
                for (var device : resources.gpu().split(",")) {
                    if (device.contains(":")) {
                        var fields = device.split(":", 2);
                        driver = fields[0];
                        count = fields[1];
                    } else {
                        driver = defDriver;
                        count = device;
                    }
                    try {
                        container.getHostConfig().getDeviceRequests().add(new DeviceRequest().withDriver(driver).
                                withCount(Integer.parseInt(count)));
                    } catch (NumberFormatException ex) {
                        throw new ISchedulerException("gpu '" + device + "' bad format", ex);
                    }

                }
            }
            container.getHostConfig().withMemory(resources.memory());
            if (resources.user() != null) {
                var fields = resources.user().split(":", 3);
                if (fields.length != 3) {
                    throw new ISchedulerException("user '" + resources.user() + "' bad format");
                }
                container.withUser(fields[1] + ":" + fields[2]);
            }
            container.getHostConfig().withReadonlyRootfs(!resources.writable());
            if (resources.tmpdir() != null) {
                String opts = "nosuid,";
                if (container.getUser() != null) {
                    opts += "uid=" + container.getUser().replace(":", ",gid=") + ",";
                }
                opts += "mode=0700,size=20m";
                container.getHostConfig().withTmpFs(Collections.singletonMap(resources.tmpdir(), opts));
            }
            if (resources.network().equals(IContainerInfo.INetworkMode.HOST)) {
                container.getHostConfig().withNetworkMode("host");
            } else {
                container.getHostConfig().withNetworkMode("bridge");
            }

            if (resources.binds() != null) {
                container.getHostConfig().withMounts(new ArrayList<>());
                for (var bind : resources.binds()) {
                    Mount mount = new Mount();
                    mount.withSource(bind.host());
                    mount.withTarget(bind.container());
                    mount.withReadOnly(bind.ro());
                    mount.withType(MountType.BIND);
                    container.getHostConfig().getMounts().add(mount);
                }
            }

            if (resources.hostnames() != null) {
                container.getHostConfig().withDns(resources.hostnames().entrySet().stream().
                        map(e -> e.getKey() + ":" + e.getValue()).toList());
            }

            env.add("IGNIS_SCHEDULER_ENV_JOB=" + jobID);
            env.add("IGNIS_SCHEDULER_ENV_CONTAINER=" + container.getName());
            if (resources.env() != null) {
                env.addAll(resources.env().entrySet().stream().
                        map(e -> e.getKey() + "=" + e.getValue()).toList());
            }
            container.withEnv(env);

            container.withLabels(new HashMap<>() {{
                put("scheduler.resources", ISchedulerUtils.encode(resources));
                put("scheduler.job", jobID);
                put("scheduler.id", container.getName());
                put("scheduler.name", ISchedulerUtils.name(request.name()));
                put("scheduler.instances", String.valueOf(request.instances()));
            }});

            //container.getHostConfig().withAutoRemove(true);TODO
            ArrayList<String> cmd = new ArrayList<>(resources.args());
            cmd.add(0, "ignis-logger");
            container.withCmd(cmd);

            containers.add(container);
        }
        return containers;
    }

    private IContainerInfo parseContainer(Container c) {
        var info = ISchedulerUtils.decode(c.getLabels().get("scheduler.resources"));

        var builder = info.toBuilder();

        builder.id(c.getNames()[0].substring(1));
        var port = new Integer[]{6000};
        if (c.getNetworkSettings() != null && c.getNetworkSettings().getNetworks().containsKey("bridge")) {
            builder.node(c.getNetworkSettings().getNetworks().get("bridge").getIpAddress());
            builder.ports(info.ports().stream().map((p) -> {
                var p2 = IPortMapping.builder();
                int container = p.container() != 0 ? p.container() : (p.host() != 0 ? p.host() : port[0]++);
                int host = p.host() != 0 ? p.host() : container;
                return p2.container(container).host(host).protocol(p.protocol()).build();
            }).toList());
        } else {
            builder.node("localhost");
        }

        return builder.build();
    }

    private List<Container> listContainers(String job, String cluster) throws ISchedulerException {
        try {
            return client.listContainersCmd().withNameFilter(
                    Collections.singleton(job + "-" + cluster)).exec();
        } catch (Exception ex) {
            throw new ISchedulerException("docker list containers error", ex);
        }
    }

    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors) throws ISchedulerException {
        String jobID = ISchedulerUtils.name(name) + "-" + ISchedulerUtils.genId();
        var containers = new ArrayList<>(parseRequest(jobID, driver));
        for (var exec : executors) {
            containers.addAll(parseRequest(jobID, exec));
        }
        var responses = new ArrayList<CreateContainerResponse>();
        try {
            for (CreateContainerCmd container : containers) {
                responses.add(container.exec());
            }
            client.startContainerCmd(responses.getLast().getId()).exec();

        } catch (NotFoundException | ConflictException ex) {
            for (var res : responses) {
                try {
                    client.killContainerCmd(res.getId());
                } catch (Exception ex2) {
                    LOGGER.debug(ex2.getMessage(), ex2);
                }
            }
            throw new ISchedulerException("docker error", ex);
        }
        return jobID;
    }

    @Override
    public void cancelJob(String id) throws ISchedulerException {
        try {
            for (var c : listContainers(id, "*")) {
                try {
                    client.removeContainerCmd(c.getId()).withForce(true).exec();
                } catch (Exception ex) {
                    LOGGER.debug("docker error", ex);
                }
            }

        } catch (Exception ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IJobInfo getJob(String id) throws ISchedulerException {
        var list = listContainers(id, "*");
        var resources = list.stream().collect(Collectors.groupingBy(c -> c.getLabels().get("scheduler.name")));
        var clusters = new ArrayList<IClusterInfo>();
        String jobID = "";
        for (var entry : resources.entrySet()) {
            jobID = entry.getValue().get(0).getLabels().get("scheduler.job");
            var name = entry.getValue().get(0).getLabels().get("scheduler.name");
            var cID = entry.getValue().get(0).getLabels().get("scheduler.id");
            var instances = Integer.parseInt(entry.getValue().get(0).getLabels().get("scheduler.instances"));
            var containers = entry.getValue().stream().sorted(Comparator.comparing(Container::getId)).
                    map(this::parseContainer).toList();
            clusters.add(IClusterInfo.builder().name(name).id(cID).instances(instances).containers(containers).build());
        }
        return IJobInfo.builder().name(jobID.split("-")[0]).id(jobID).clusters(clusters).build();
    }

    @Override
    public List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException {
        return null;
    }

    @Override
    public IClusterInfo createCluster(String job, IClusterRequest resources) throws ISchedulerException {
        return null;
    }

    @Override
    public void destroyCluster(String job, String id) throws ISchedulerException {
        try {
            for (var c : listContainers(job, id)) {
                try {
                    client.removeContainerCmd(c.getId()).withForce(true).exec();
                } catch (Exception ex) {
                    LOGGER.debug("docker error", ex);
                }
            }

        } catch (Exception ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IClusterInfo getCluster(String job, String id) throws ISchedulerException {
        try {
            var list = listContainers(job, id);
            if (list.isEmpty()) {
                throw new ISchedulerException("cluster not found");
            }
            var name = list.get(0).getLabels().get("scheduler.name");
            var resources = list.stream().map(this::parseContainer).toList();
            var instances = Integer.parseInt(list.get(0).getLabels().get("scheduler.instances"));
            return IClusterInfo.builder().name(name).id(id).instances(instances).containers(resources).build();
        } catch (Exception ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IClusterInfo repairCluster(String job, IClusterInfo cluster) throws ISchedulerException {
        return null;
    }

    @Override
    public IContainerInfo.IStatus getContainerStatus(String job, String id) throws ISchedulerException {
        try {
            var list = listContainers(job, id);
            if (list.isEmpty()) {
                return IContainerInfo.IStatus.DESTROYED;
            }
            return DOCKER_STATUS.getOrDefault(list.get(0).getState(), IContainerInfo.IStatus.UNKNOWN);
        } catch (Exception ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        try {
            client.pingCmd().exec();
        } catch (DockerException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }
}
