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
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.ignis.scheduler.model.*;

import java.io.File;
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
        if (!url.contains("://")) {
            if (url.startsWith("/")) {
                url = "unix://" + url;
            } else if (url.contains(":") && !url.startsWith("tcp://")) {
                url = "tcp://" + url;
            }
        }
        if (url.toLowerCase().startsWith("unix:")) {
            this.path = new File(url.substring("unix:".length())).getAbsolutePath();
        } else {
            this.path = null;
        }
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

    private String fixName(String name) {
        return name.toLowerCase().replaceAll("[^\\w\\-\\._\\\\]", "");
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
                if (port.getContainerPort() == 0 && port.getHostPort() == 0) {
                    value += dynPort + ":" + dynPort;
                    dynPort++;
                } else {
                    if (port.getContainerPort() == 0) {
                        value += port.getContainerPort();
                    }
                    if (port.getHostPort() == 0) {
                        value += port.getContainerPort();
                    }
                    value = value + ":" + value;
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
        builder.id(container.getName().substring(1));
        builder.host(container.getNetworkSettings().getNetworks().get("bridge").getIpAddress());
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
            Map<String, InspectContainerResponse.Mount> mountInfo = new HashMap<>();
            for (InspectContainerResponse.Mount mount : container.getMounts()) {
                mountInfo.put(mount.getDestination().getPath(), mount);
            }
            for (Mount mount : container.getHostConfig().getMounts()) {
                if (mount.getType() == MountType.BIND) {
                    binds.add(new IBind(mount.getSource(), mount.getTarget(), !mountInfo.get(mount.getTarget()).getRW()));
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

    private List<String> getDockerIds(List<String> ids) throws ISchedulerException {
        return getDockerIds(ids, true);
    }

    private List<String> getDockerIds(List<String> ids, boolean safe) throws ISchedulerException {
        Map<String, String> map = new HashMap<>();
        List<String> result = new ArrayList<>();
        List<Container> containers = dockerClient.listContainersCmd().withNameFilter(ids).exec();
        for (Container c : containers) {
            map.put(c.getNames()[0].substring(1), c.getId());
        }
        for (String id : ids) {
            if (!map.containsKey(id)) {
                if (safe) {
                    throw new ISchedulerException("Container " + id + " not found");
                }
                result.add(null);
            } else {
                result.add(map.get(id));
            }
        }
        return result;
    }

    @Override
    public String createGroup(String name) throws ISchedulerException {
        return fixName(name + "-" + newId());
    }

    @Override
    public void destroyGroup(String group) throws ISchedulerException {
    }

    @Override
    public String createDriverContainer(String group, String name, IContainerInfo container) throws ISchedulerException {
        try {
            CreateContainerCmd dockerContainer = parseContainer(container);
            dockerContainer.withName(fixName(group + "-" + name));
            String[] env = Arrays.copyOf(dockerContainer.getEnv(), dockerContainer.getEnv().length + 2);
            env[env.length - 2] = "IGNIS_JOB_ID=" + dockerContainer.getName();
            env[env.length - 1] = "IGNIS_JOB_NAME=" + group;
            dockerContainer.withEnv(env);
            ArrayList<String> cmd = new ArrayList<>();
            cmd.add("ignis-log");
            cmd.add("$IGNIS_WORKING_DIRECTORY/" + dockerContainer.getName() + ".out");
            cmd.add("$IGNIS_WORKING_DIRECTORY/" + dockerContainer.getName() + ".err");
            cmd.addAll(Arrays.asList(dockerContainer.getCmd()));
            dockerContainer.withCmd(cmd);
            if (path != null) {//Is a Unix-Socket
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
            return dockerContainer.getName();
        } catch (DockerException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<String> createExecutorContainers(String group, String name, IContainerInfo container, int instances) throws ISchedulerException {
        List<String> ids = new ArrayList<>();
        List<String> names = new ArrayList<>();
        String IGNIS_WORKING_DIRECTORY = System.getenv("IGNIS_WORKING_DIRECTORY");
        try {
            CreateContainerCmd dockerContainer = parseContainer(container);
            for (int i = 0; i < instances; i++) {
                dockerContainer.withName(fixName(group + "-" + name + "." + i));
                ArrayList<String> cmd = new ArrayList<>();
                cmd.add("ignis-log");
                cmd.add(IGNIS_WORKING_DIRECTORY + "/" + dockerContainer.getName() + ".out");
                cmd.add(IGNIS_WORKING_DIRECTORY + "/" + dockerContainer.getName() + ".err");
                cmd.addAll(Arrays.asList(dockerContainer.getCmd()));
                dockerContainer.withCmd(cmd);
                names.add(dockerContainer.getName());
                ids.add(dockerContainer.exec().getId());
            }
            for (int i = 0; i < instances; i++) {
                dockerClient.startContainerCmd(ids.get(i)).exec();
            }
            return names;
        } catch (DockerException ex) {
            try {
                destroyExecutorInstances(names);
            } catch (ISchedulerException ex2) {
            }
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IContainerStatus getStatus(String id) throws ISchedulerException {
        return getStatus(List.of(id)).get(0);
    }

    @Override
    public List<IContainerStatus> getStatus(List<String> ids) throws ISchedulerException {
        try {
            List<IContainerStatus> status = new ArrayList<>();
            for (String id : getDockerIds(ids, false)) {
                if (id == null) {
                    status.add(IContainerStatus.DESTROYED);
                } else {
                    String s = dockerClient.inspectContainerCmd(id).exec().getState().getStatus();
                    status.add(TASK_STATUS.getOrDefault(s, IContainerStatus.UNKNOWN));
                }
            }
            return status;
        } catch (DockerException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IContainerInfo getDriverContainer(String id) throws ISchedulerException {
        return getExecutorContainers(List.of(id)).get(0);
    }

    @Override
    public List<IContainerInfo> getExecutorContainers(List<String> ids) throws ISchedulerException {
        List<IContainerInfo> result = new ArrayList<>();
        for (String id : getDockerIds(ids)) {
            result.add(parseContainer(dockerClient.inspectContainerCmd(id).exec()));
        }
        return result;
    }

    @Override
    public IContainerInfo restartContainer(String id) throws ISchedulerException {
        String dockerid = getDockerIds(List.of(id)).get(0);
        try {
            try {
                if (getStatus(id) == IContainerStatus.RUNNING) {
                    return getDriverContainer(id);
                }
                dockerClient.stopContainerCmd(dockerid).exec();
            } catch (Exception ignore) {
            }
            dockerClient.startContainerCmd(dockerid).exec();
            return getDriverContainer(id);
        } catch (DockerException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroyDriverContainer(String id) throws ISchedulerException {
        destroyExecutorInstances(List.of(id));
    }

    @Override
    public void destroyExecutorInstances(List<String> ids) throws ISchedulerException {
        DockerException error = null;
        for (String id : getDockerIds(ids, false)) {
            try {
                if (id != null) {
                    dockerClient.removeContainerCmd(id).withForce(true).withRemoveVolumes(true).exec();
                }
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
