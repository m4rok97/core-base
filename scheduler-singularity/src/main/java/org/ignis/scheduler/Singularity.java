/*
 * Copyright (C) 2020 César Pomar
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.hubspot.horizon.HttpClient;
import com.hubspot.horizon.HttpConfig;
import com.hubspot.horizon.ning.NingHttpClient;
import com.hubspot.mesos.*;
import com.hubspot.singularity.*;
import com.hubspot.singularity.client.SingularityClientException;
import org.ignis.scheduler.model.*;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class Singularity implements IScheduler {
    public static final String NAME = "singularity";

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Singularity.class);

    private final SingularityClientExt client;
    private final ObjectMapper objectMapper;

    private final static Map<ExtendedTaskState, IContainerStatus> TASK_STATUS = new HashMap<>() {
        {
            put(ExtendedTaskState.TASK_LAUNCHED, IContainerStatus.ACCEPTED);
            put(ExtendedTaskState.TASK_STAGING, IContainerStatus.ACCEPTED);
            put(ExtendedTaskState.TASK_STARTING, IContainerStatus.ACCEPTED);
            put(ExtendedTaskState.TASK_RUNNING, IContainerStatus.RUNNING);
            put(ExtendedTaskState.TASK_CLEANING, IContainerStatus.DESTROYED);
            put(ExtendedTaskState.TASK_KILLING, IContainerStatus.DESTROYED);
            put(ExtendedTaskState.TASK_FINISHED, IContainerStatus.FINISHED);
            put(ExtendedTaskState.TASK_FAILED, IContainerStatus.ERROR);
            put(ExtendedTaskState.TASK_KILLED, IContainerStatus.DESTROYED);
            put(ExtendedTaskState.TASK_LOST, IContainerStatus.DESTROYED);
            put(ExtendedTaskState.TASK_LOST_WHILE_DOWN, IContainerStatus.DESTROYED);
            put(ExtendedTaskState.TASK_ERROR, IContainerStatus.ERROR);
            put(ExtendedTaskState.TASK_DROPPED, IContainerStatus.ERROR);
            put(ExtendedTaskState.TASK_GONE, IContainerStatus.FINISHED);
            put(ExtendedTaskState.TASK_UNREACHABLE, IContainerStatus.UNKNOWN);
            put(ExtendedTaskState.TASK_GONE_BY_OPERATOR, IContainerStatus.UNKNOWN);
            put(ExtendedTaskState.TASK_UNKNOWN, IContainerStatus.UNKNOWN);
        }
    };

    public Singularity(String url) {
        List<String> hosts;
        if (url.contains(",")) {
            hosts = Arrays.asList(url.split(","));
        } else {
            hosts = Arrays.asList(url);
        }

        String contextPath = "api";
        HttpConfig config = HttpConfig.newBuilder().build();
        objectMapper = config.getObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        HttpClient httpClient = new NingHttpClient(config);
        client = new SingularityClientExt(contextPath, httpClient, hosts, Optional.empty());
    }

    private String fixSingularityId(String id) {
        return id.toLowerCase().replaceAll("[^\\w_\\.]", "");
    }

    private String newId() {
        UUID id = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 + 1);
        buffer.put((byte) 0);
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        return new BigInteger(buffer.array()).toString(36);
    }

    private SingularityDeployBuilder createDeploy(String group, String id, IContainerInfo container) {
        SingularityDeployBuilder deployBuilder = new SingularityDeployBuilder("", id);


        List<SingularityVolume> deployVolumes = new ArrayList<>();
        List<SingularityDockerPortMapping> deployPortMappings = new ArrayList<>();
        Map<String, String> deployParameters = new HashMap<>();
        List<SingularityDockerParameter> deployDockerParameters = new ArrayList<>();
        deployBuilder.setMetadata(Optional.of(new HashMap<>()));

        //Not supported by must store the value
        if (container.getPreferedHosts() != null) {
            deployBuilder.getMetadata().get().put("prefered-hosts",
                    container.getPreferedHosts().stream().collect(Collectors.joining(",")));
        }

        deployBuilder.setContainerInfo(Optional.of(new SingularityContainerInfo(
                SingularityContainerType.DOCKER,
                Optional.of(deployVolumes),
                Optional.of(
                        new SingularityDockerInfo(
                                container.getImage(),
                                false, /*privileged*/
                                SingularityDockerNetworkType.BRIDGE,
                                Optional.of(deployPortMappings),
                                Optional.of(false), /*forcePullImage*/
                                Optional.of(deployParameters),
                                Optional.of(deployDockerParameters)
                        )
                )
        )));

        if (container.getSwappiness() != null) {
            deployDockerParameters.add(new SingularityDockerParameter("memory-swappiness", "" + container.getSwappiness()));
            deployBuilder.getMetadata().get().put("memory-swappiness", "" + container.getSwappiness());
        }

        deployBuilder.setShell(Optional.of(true));
        deployBuilder.getMetadata().get().put("cmd", container.getCommand());
        List<String> cmds = new ArrayList<>();
        cmds.add(container.getCommand());
        if (container.getArguments() != null) {
            deployBuilder.getMetadata().get().put("args", String.valueOf(container.getArguments().size()));
            for (int i = 0; i < container.getArguments().size(); i++) {
                deployBuilder.getMetadata().get().put("arg" + i, container.getArguments().get(i));
            }
            cmds.addAll(container.getArguments());
        }
        StringBuilder cmdBuilder = new StringBuilder();
        for (String cmd : cmds) {
            cmdBuilder.append('"');
            cmdBuilder.append(cmd.replace("\"", "\\\""));
            cmdBuilder.append('"').append(' ');
        }
        deployBuilder.setCommand(Optional.of(cmdBuilder.toString()));

        int numPorts = 0;
        if (container.getPorts() != null) {
            Optional<SingularityPortMappingType> literal = Optional.of(SingularityPortMappingType.LITERAL);
            Optional<SingularityPortMappingType> offer = Optional.of(SingularityPortMappingType.FROM_OFFER);
            for (IPort port : container.getPorts()) {
                deployPortMappings.add(new SingularityDockerPortMapping(
                        port.getContainerPort() == 0 ? offer : literal,
                        port.getContainerPort() != 0 ? port.getContainerPort() : numPorts,
                        port.getHostPort() == 0 ? offer : literal,
                        port.getHostPort() != 0 ? port.getHostPort() : numPorts,
                        Optional.of(port.getProtocol())
                ));
                if (port.getContainerPort() == 0 || port.getHostPort() == 0) {
                    numPorts++;
                }
            }
        }

        deployBuilder.setResources(Optional.of(new Resources(
                container.getCpus(), /*cpus*/
                (double) container.getMemory() / (1000 * 1000), /*memoryMb*/
                numPorts,/*numPorts*/
                0/*diskMb*/
        )));

        if (container.getHostnames() != null) {
            deployBuilder.getMetadata().get().put("hostnames",
                    container.getHostnames().stream().collect(Collectors.joining(",")));
            for (String hostname : container.getHostnames()) {
                deployDockerParameters.add(new SingularityDockerParameter("add-host", hostname));
            }
        }

        if (container.getBinds() != null) {
            for (IBind bind : container.getBinds()) {
                deployVolumes.add(new SingularityVolume(
                        bind.getContainerPath(),
                        Optional.of(bind.getHostPath()),
                        bind.isReadOnly() ? SingularityDockerVolumeMode.RO : SingularityDockerVolumeMode.RW,
                        Optional.empty()
                ));
            }
        }

        if (container.getVolumes() != null) {
            for (IVolume vol : container.getVolumes()) {
                deployVolumes.add(new SingularityVolume(
                        vol.getContainerPath(),
                        Optional.empty(),
                        SingularityDockerVolumeMode.RW,
                        Optional.of(new SingularityVolumeSource(
                                SingularityVolumeSourceType.DOCKER_VOLUME,
                                Optional.of(new SingularityDockerVolume(
                                        Optional.of("local"),
                                        Optional.empty(),
                                        new HashMap<>() {{
                                            put("size", "" + vol.getSize());
                                        }}
                                ))
                        ))
                ));
            }
        }

        Map<String, String> deployEnv = new HashMap<>();
        deployBuilder.setEnv(Optional.of(deployEnv));
        deployEnv.put("IGNIS_JOB_NAME", id);
        deployEnv.put("IGNIS_JOB_GROUP", group);
        if (System.getenv("TZ") != null) { //Copy timezone
            deployEnv.put("TZ", System.getenv("TZ"));
        }
        if (container.getEnvironmentVariables() != null) {
            deployEnv.putAll(container.getEnvironmentVariables());
        }

        if (Boolean.getBoolean("ignis.debug")) {
            try {
                LOGGER.info("Debug: " + objectMapper.writeValueAsString(deployBuilder.build()));
            } catch (JsonProcessingException e) {
                LOGGER.info("Debug: " + deployBuilder.build().toString());
            }
        }

        return deployBuilder;
    }

    private IContainerInfo parseDeploy(SingularityTask task) {
        SingularityDeploy deploy = task.getTaskRequest().getDeploy();
        Map<String, String> meta = deploy.getMetadata().get();
        Resources resources = deploy.getResources().get();
        SingularityContainerInfo container = deploy.getContainerInfo().get();
        IContainerInfo.IContainerInfoBuilder builder = IContainerInfo.builder();

        builder.id(deploy.getRequestId());
        builder.host(task.getHostname());
        builder.image(container.getDocker().get().getImage());
        builder.command(meta.get("cmd"));
        List<String> args = new ArrayList<>();
        builder.arguments(args);
        if (meta.containsKey("args")) {
            for (int i = 0; i < Integer.parseInt(meta.get("args")); i++) {
                args.add(meta.get("arg" + i));
            }
        }
        builder.cpus((int) resources.getCpus());
        builder.memory((long) resources.getMemoryMb() * 1000 * 1000);
        if (meta.containsKey("memory-swappiness")) {
            builder.swappiness(Integer.parseInt(meta.get("memory-swappiness")));
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> mesosContainer = (Map<String, Object>) task.getMesosTask().getAllOtherFields().get("container");
        @SuppressWarnings("unchecked")
        Map<String, Object> mesosDockerContainer = (Map<String, Object>) mesosContainer.get("docker");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> mesosPortMappings = (List<Map<String, Object>>) mesosDockerContainer.get("portMappings");
        List<IPort> ports = new ArrayList<>();
        builder.ports(ports);
        for (Map<String, Object> port : mesosPortMappings) {
            ports.add(new IPort((int) port.get("containerPort"), (int) port.get("hostPort"), (String) port.get("protocol")));
        }
        List<IBind> binds = new ArrayList<>();
        List<IVolume> volumes = new ArrayList<>();
        builder.binds(binds);
        builder.volumes(volumes);
        for (SingularityVolume volume : container.getVolumes().get()) {
            if (volume.getSource().isPresent()) {
                String size = volume.getSource().get().getDockerVolume().get().getDriverOptions().get("size");
                volumes.add(new IVolume(
                        volume.getContainerPath(),
                        Long.parseLong(size)
                ));
            } else {
                binds.add(new IBind(
                        volume.getHostPath().get(),
                        volume.getContainerPath(),
                        volume.getMode().get() == SingularityDockerVolumeMode.RO
                ));
            }
        }
        if (meta.containsKey("prefered-hosts")) {
            builder.preferedHosts(Arrays.asList(meta.get("prefered-hosts").split(",")));
        }
        if (meta.containsKey("hostnames")) {
            builder.hostnames(Arrays.asList(meta.get("hostnames").split(",")));
        }
        builder.environmentVariables(deploy.getEnv().get());
        Map<String, String> params = new HashMap<>();
        builder.schedulerParams(params);

        return builder.build();
    }

    @Override
    public String createGroup(String name) throws ISchedulerException {
        try {
            String id = fixSingularityId(name);
            SingularityRequestGroup requestGroup = new SingularityRequestGroup(id, new ArrayList<>(), null);
            client.saveRequestGroup(requestGroup);
            return id;
        } catch (SingularityClientException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroyGroup(String group) throws ISchedulerException {
        try {
            client.deleteRequestGroup(group);
        } catch (SingularityClientException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public String createDriverContainer(String group, String name, IContainerInfo container) throws ISchedulerException {
        try {
            return createExecutorContainers(group, name, container, 1).get(0);
        } catch (SingularityClientException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<String> createExecutorContainers(String group, String name, IContainerInfo container, int instances) throws ISchedulerException {
        try {
            SingularityDeployBuilder deployBuilder = createDeploy(group, "0", container);
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < instances; i++) {
                String id = fixSingularityId(name) + "-" + i + "-" + newId();
                SingularityRequestBuilder requestBuilder = new SingularityRequestBuilder(id, RequestType.RUN_ONCE);
                requestBuilder.setGroup(Optional.of(group));
                requestBuilder.setInstances(Optional.of(instances));
                deployBuilder.setRequestId(id);

                client.createOrUpdateSingularityRequest(requestBuilder.build());
                client.createDeployForSingularityRequest(id, deployBuilder.build(), Optional.empty(), Optional.empty());
                ids.add(id);
            }
            return ids;
        } catch (SingularityClientException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IContainerStatus getStatus(String id) throws ISchedulerException {
        try {
            Optional<SingularityRequestParent> request = client.getSingularityRequest(id);
            if (request.isEmpty() || request.get().getTaskIds().isEmpty()) {
                return IContainerStatus.ACCEPTED;
            }

            SingularityTaskIdsByStatus tasks = request.get().getTaskIds().get();
            String taskId;

            if (!tasks.getHealthy().isEmpty()) {
                taskId = tasks.getHealthy().get(0).getId();
            } else if (!tasks.getNotYetHealthy().isEmpty()) {
                taskId = tasks.getNotYetHealthy().get(0).getId();
            } else {
                Collection<SingularityTaskIdHistory> dead = client.
                        getInactiveTaskHistoryForRequest(id, request.get().getActiveDeploy().get().getId());
                if (dead.isEmpty()) {
                    return IContainerStatus.ACCEPTED;
                } else {
                    taskId = dead.iterator().next().getTaskId().getId();
                }
            }

            Optional<SingularityTaskState> taskState = client.getTaskState(taskId);
            return TASK_STATUS.getOrDefault(taskState.get().getCurrentState().get(), IContainerStatus.UNKNOWN);
        } catch (SingularityClientException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<IContainerStatus> getStatus(List<String> ids) throws ISchedulerException {
        List<IContainerStatus> result = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            result.add(getStatus(ids.get(i)));
        }
        return result;
    }

    @Override
    public IContainerInfo getDriverContainer(String id) throws ISchedulerException {
        try {
            long time = 1;
            while (true) {
                Thread.sleep(time * 1000);
                if (time < 30) {
                    time++;
                }

                Optional<SingularityRequestParent> request = client.getSingularityRequest(id);
                if (request.isEmpty() || request.get().getTaskIds().isEmpty()) {
                    continue;
                }

                SingularityTaskIdsByStatus tasks = request.get().getTaskIds().get();
                List<SingularityTaskId> aux = new ArrayList<>();
                aux.addAll(tasks.getHealthy());
                aux.addAll(tasks.getNotYetHealthy());
                if (aux.isEmpty()) {
                    if (request.get().getActiveDeploy().isPresent() &&
                            !client.getInactiveTaskHistoryForRequest(id, request.get().getActiveDeploy().get().getId()).isEmpty()) {
                        throw new ISchedulerException("Container dead before stating");
                    }
                    continue;
                }
                return parseDeploy(client.getSingularityTask(aux.get(0).getId()).get());

            }
        } catch (SingularityClientException | InterruptedException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<IContainerInfo> getExecutorContainers(List<String> ids) throws ISchedulerException {
        List<IContainerInfo> result = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            LOGGER.info("Waiting cluster deployment..." + i + " of " + ids.size());
            result.add(getDriverContainer(ids.get(i)));
        }
        return result;
    }

    @Override
    public IContainerInfo restartContainer(String id) throws ISchedulerException {
        try {
            Collection<SingularityTaskIdHistory> active = client.getActiveTaskHistoryForRequest(id);
            for (SingularityTaskIdHistory task : active) {
                client.killTask(task.getTaskId().getId(), Optional.empty());
            }
            Optional<SingularityRequestParent> request = client.getSingularityRequest(id);
            SingularityDeployBuilder builder = request.get().getActiveDeploy().get().toBuilder();
            builder.setId("" + (Integer.parseInt(builder.getId()) + 1));

            client.createDeployForSingularityRequest(
                    request.get().getRequest().getId(),
                    builder.build(),
                    Optional.empty(),
                    Optional.empty()
            );
            return getDriverContainer(id);
        } catch (SingularityClientException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroyDriverContainer(String id) throws ISchedulerException {
        try {
            client.deleteSingularityRequest(id, Optional.empty());
        } catch (SingularityClientException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroyExecutorInstances(List<String> ids) throws ISchedulerException {
        for (String id : ids) {
            destroyDriverContainer(id);
        }
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        try {
            client.getState(Optional.of(true), Optional.of(false));
        } catch (SingularityClientException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}
