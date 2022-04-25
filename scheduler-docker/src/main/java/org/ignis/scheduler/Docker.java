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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.DockerException;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.ignis.scheduler.model.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author César Pomar
 */
public class Docker implements IScheduler {

    public static final String NAME = "docker";
    private final static Map<String, IContainerStatus> TASK_STATUS = new HashMap<>() {
        {
            put("created", IContainerStatus.ACCEPTED);
            put("restarting", IContainerStatus.ACCEPTED);
            put("running", IContainerStatus.RUNNING);
            put("exited", IContainerStatus.FINISHED);
            put("removing", IContainerStatus.DESTROYED);
            put("paused", IContainerStatus.ERROR);
            put("dead", IContainerStatus.ERROR);
        }
    };

    private final String path;
    private final DockerClientConfig config;
    private final DockerHttpClient httpClient;
    private final DockerClient dockerClient;

    public Docker(String url) {
        this.path = url;
        config = DefaultDockerClientConfig.
                createDefaultConfigBuilder().
                withDockerHost(url).
                build();

        httpClient = new ApacheDockerHttpClient.Builder().
                dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .maxConnections(100)
                .connectionTimeout(Duration.ofSeconds(30))
                .responseTimeout(Duration.ofSeconds(45))
                .build();

        dockerClient = DockerClientImpl.getInstance(config, httpClient);
    }

    private String newId() {
        UUID id = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 + 1);
        buffer.put((byte) 0);
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        return new BigInteger(buffer.array()).toString(36);
    }

    private CreateContainerCmd parseContainer(IContainerInfo container) {
        CreateContainerCmd dockerContainer = dockerClient.createContainerCmd(container.getImage());
        dockerContainer.withLabels(new HashMap<>());
        dockerContainer.withHostConfig(new HostConfig());
        List<String> cmd = new ArrayList<>();
        cmd.add(container.getCommand());
        if (container.getArguments() != null) {
            cmd.addAll(container.getArguments());
        }
        dockerContainer.withCmd(cmd);
        dockerContainer.getHostConfig().withCpuCount((long) container.getCpus());
        dockerContainer.getHostConfig().withMemory(container.getMemory());
        if (container.getSwappiness() != null) {
            dockerContainer.getHostConfig().withMemorySwappiness((long) container.getSwappiness());
        }

        /*
         * Ports are not exposed because all container are in the host inner network
         * */
        dockerContainer.getHostConfig().withNetworkMode("bridge");
        int dynPort = 32000;
        if (container.getPorts() != null) {
            dockerContainer.getLabels().put("ports", "" + container.getPorts().size());
            int i = 0;
            for (IPort port : container.getPorts()) {
                String value = "";
                boolean flag = false;
                if (port.getContainerPort() == 0) {
                    value += dynPort;
                    flag = true;
                } else {
                    value += port.getContainerPort();
                }
                value += ":";
                if (port.getHostPort() == 0) {
                    value += dynPort;
                    flag = true;
                } else {
                    value += port.getHostPort();
                }
                if (flag) {
                    dynPort++;
                }
                value += ":" + port.getProtocol();
                dockerContainer.getLabels().put("port" + i, value);
                i++;
            }
        }

        List<Mount> mounts = new ArrayList<>();
        if (container.getBinds() != null) {
            for (IBind bind : container.getBinds()) {
                Mount mount = new Mount();
                mount.withSource(bind.getHostPath());
                mount.withTarget(bind.getContainerPath());
                mount.withReadOnly(bind.isReadOnly());
                mount.withType(MountType.BIND);
                mounts.add(mount);
            }
        }

        if (container.getVolumes() != null) {
            for (IVolume volume : container.getVolumes()) {
                Mount mount = new Mount();
                VolumeOptions volumeOptions = new VolumeOptions();
                Driver driver = new Driver();
                mount.withTarget(volume.getContainerPath());
                mount.withVolumeOptions(volumeOptions);
                mount.withType(MountType.VOLUME);
                volumeOptions.withDriverConfig(driver);
                driver.withOptions(Map.of("size", "" + volume.getSize()));
                mounts.add(mount);
            }
        }
        dockerContainer.getHostConfig().withMounts(mounts);
        if (container.getPreferedHosts() != null) {
            dockerContainer.getLabels().put("prefered-hosts",
                    container.getPreferedHosts().stream().collect(Collectors.joining(",")));
        }
        if (container.getHostnames() != null) {
            dockerContainer.getHostConfig().withDns(container.getHostnames());
        }
        Map<String, String> env = new HashMap<>();
        if (System.getenv("TZ") != null) { //Copy timezone
            env.put("TZ", System.getenv("TZ"));
        }
        if (container.getEnvironmentVariables() != null) {
            env.putAll(container.getEnvironmentVariables());
        }
        dockerContainer.withEnv(env.entrySet().stream().map((e) -> e.getKey() + "=" + e.getValue()).collect(Collectors.toList()));

        return dockerContainer;
    }

    private IContainerInfo parseContainer(InspectContainerResponse container) {
        IContainerInfo.IContainerInfoBuilder builder = IContainerInfo.builder();
        Map<String, String> labels = container.getConfig().getLabels();
        builder.id(container.getId());
        builder.host(container.getConfig().getHostName());
        builder.image(container.getConfig().getImage());
        builder.command(container.getConfig().getCmd()[0]);
        List<String> args = new ArrayList<>();
        for (int i = 1; i < container.getConfig().getCmd().length; i++) {
            args.add(container.getConfig().getCmd()[i]);
        }
        builder.arguments(args);
        builder.cpus(container.getHostConfig().getCpuCount().intValue());
        builder.memory(container.getHostConfig().getMemory());
        if (container.getHostConfig().getMemorySwappiness() != null) {
            builder.swappiness(container.getHostConfig().getMemorySwappiness().intValue());
        }

        List<IPort> ports = new ArrayList<>();
        builder.ports(ports);
        builder.networkMode(INetworkMode.BRIDGE);
        if (labels.containsKey("ports")) {
            int n = Integer.parseInt(labels.get("ports"));
            for (int i = 0; i < n; i++) {
                String[] aux = labels.get("port" + i).split(":");
                ports.add(new IPort(Integer.parseInt(aux[0]), Integer.parseInt(aux[1]), aux[2]));
            }
        }
        List<IBind> binds = new ArrayList<>();
        List<IVolume> volumes = new ArrayList<>();
        builder.binds(binds);
        builder.volumes(volumes);
        if (container.getHostConfig().getMounts() != null) {
            for (Mount mount : container.getHostConfig().getMounts()) {
                if (mount.getType() == MountType.BIND) {
                    binds.add(new IBind(mount.getSource(), mount.getTarget(), mount.getReadOnly()));
                } else if (mount.getType() == MountType.VOLUME) {
                    String size = mount.getVolumeOptions().getDriverConfig().getOptions().get("size");
                    volumes.add(new IVolume(mount.getTarget(), Long.parseLong(size)));
                }
            }
        }

        if (labels.containsKey("prefered-hosts")) {
            builder.preferedHosts(Arrays.asList(labels.get("prefered-hosts").split(",")));
        }

        if (container.getHostConfig().getDns() != null) {
            builder.hostnames(Arrays.asList(container.getHostConfig().getDns()));
        }

        Map<String, String> envs = new HashMap<>();
        builder.environmentVariables(envs);
        if (container.getConfig().getEnv() != null) {
            for (String env : container.getConfig().getEnv()) {
                String[] aux = env.split("=");
                envs.put(aux[0], aux[1]);
            }
        }
        Map<String, String> params = new HashMap<>();
        builder.schedulerParams(params);

        return builder.build();
    }

    @Override
    public String createGroup(String name) throws ISchedulerException {
        return name + "-" + newId();
    }

    @Override
    public void destroyGroup(String group) throws ISchedulerException {
    }

    @Override
    public String createDriverContainer(String group, String name, IContainerInfo container) throws ISchedulerException {
        try {
            CreateContainerCmd dockerContainer = parseContainer(container);
            String[] env = Arrays.copyOf(dockerContainer.getEnv(), dockerContainer.getEnv().length + 2);
            env[env.length - 2] = "IGNIS_JOB_ID=" + dockerContainer.getName();
            env[env.length - 1] = "IGNIS_JOB_NAME=" + group;
            dockerContainer.withEnv(env);
            dockerContainer.withEnv(env);
            if (path.startsWith("/")) {//Is a Unix-Socket
                List<Mount> mounts = dockerContainer.getHostConfig().getMounts();
                Mount mount = new Mount();
                mount.withSource(path);
                mount.withTarget(path);
                mount.withReadOnly(false);
                mount.withType(MountType.BIND);
                mounts.add(mount);
            }
            String id = dockerContainer.exec().getId();
            dockerClient.startContainerCmd(id).exec();
            return id;
        } catch (DockerException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<String> createExecutorContainers(String group, String name, IContainerInfo container, int instances) throws ISchedulerException {
        List<String> ids = new ArrayList<>();
        try {
            CreateContainerCmd dockerContainer = parseContainer(container);
            for (int i = 0; i < instances; i++) {
                dockerContainer.withName(group + "-" + name + "." + i);
                ids.add(dockerContainer.exec().getId());
            }
            return ids;
        } catch (DockerException ex) {
            try {
                destroyExecutorInstances(ids);
            } catch (ISchedulerException ex2) {
            }
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IContainerStatus getStatus(String id) throws ISchedulerException {
        try {
            List<Container> containers = dockerClient.listContainersCmd().withIdFilter(List.of(id)).exec();
            if (containers.isEmpty()) {
                return IContainerStatus.DESTROYED;
            }
            return TASK_STATUS.getOrDefault(containers.get(0).getState(), IContainerStatus.UNKNOWN);
        } catch (DockerException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<IContainerStatus> getStatus(List<String> ids) throws ISchedulerException {
        try {
            List<IContainerStatus> status = new ArrayList<>();
            Map<String, Container> map = new HashMap<>();
            List<Container> containers = dockerClient.listContainersCmd().withIdFilter(ids).exec();
            for (Container c : containers) {
                map.put(c.getId(), c);
            }
            for (String id : ids) {
                if (!map.containsKey(id)) {
                    status.add(IContainerStatus.DESTROYED);
                } else {
                    status.add(TASK_STATUS.getOrDefault(map.get(id).getState(), IContainerStatus.UNKNOWN));
                }
            }
            return status;
        } catch (DockerException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IContainerInfo getDriverContainer(String id) throws ISchedulerException {
        try {
            return parseContainer(dockerClient.inspectContainerCmd(id).exec());
        } catch (DockerException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<IContainerInfo> getExecutorContainers(List<String> ids) throws ISchedulerException {
        List<IContainerInfo> result = new ArrayList<>();
        for (String id : ids) {
            result.add(getDriverContainer(id));
        }
        return result;
    }

    @Override
    public IContainerInfo restartContainer(String id) throws ISchedulerException {
        try {
            try {
                if (getStatus(id) == IContainerStatus.RUNNING) {
                    return getDriverContainer(id);
                }
                dockerClient.stopContainerCmd(id).exec();
            } catch (Exception ignore) {
            }
            dockerClient.startContainerCmd(id).exec();
            return getDriverContainer(id);
        } catch (DockerException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroyDriverContainer(String id) throws ISchedulerException {
        try {
            dockerClient.removeContainerCmd(id).withForce(true).withRemoveVolumes(true).exec();
        } catch (NotFoundException ex) {
            //Ignore
        } catch (DockerException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroyExecutorInstances(List<String> ids) throws ISchedulerException {
        DockerException error = null;
        for (String id : ids) {
            try {
                dockerClient.removeContainerCmd(id).withForce(true).withRemoveVolumes(true).exec();
            } catch (NotFoundException ex) {
                //Ignore
            } catch (DockerException ex) {
                error = ex;
            }
        }
        if (error != null) {
            throw new ISchedulerException(error.getMessage(), error);
        }
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        try {
            dockerClient.pingCmd().exec();
        } catch (DockerException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}
