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
package org.ignis.backend.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.gson.Gson;
import com.hubspot.horizon.HttpClient;
import com.hubspot.horizon.HttpConfig;
import com.hubspot.horizon.ning.NingHttpClient;
import com.hubspot.mesos.*;
import com.hubspot.singularity.*;
import com.hubspot.singularity.client.SingularityClient;
import com.hubspot.singularity.client.SingularityClientException;

import org.ignis.backend.exception.ISchedulerException;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.scheduler.model.IBind;
import org.ignis.backend.scheduler.model.IContainerDetails;
import org.ignis.backend.scheduler.model.IPort;
import org.ignis.backend.scheduler.model.IVolume;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ISingularityScheduler implements IScheduler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ISingularityScheduler.class);

    private final SingularityClient client;
    private final static Map<ExtendedTaskState, IContainerDetails.ContainerStatus> TASK_STATUS = new HashMap<>() {
        {
            put(ExtendedTaskState.TASK_LAUNCHED, IContainerDetails.ContainerStatus.ACCEPTED);
            put(ExtendedTaskState.TASK_STAGING, IContainerDetails.ContainerStatus.ACCEPTED);
            put(ExtendedTaskState.TASK_STARTING, IContainerDetails.ContainerStatus.ACCEPTED);
            put(ExtendedTaskState.TASK_RUNNING, IContainerDetails.ContainerStatus.RUNNING);
            put(ExtendedTaskState.TASK_CLEANING, IContainerDetails.ContainerStatus.DESTROYED);
            put(ExtendedTaskState.TASK_KILLING, IContainerDetails.ContainerStatus.DESTROYED);
            put(ExtendedTaskState.TASK_FINISHED, IContainerDetails.ContainerStatus.FINISHED);
            put(ExtendedTaskState.TASK_FAILED, IContainerDetails.ContainerStatus.ERROR);
            put(ExtendedTaskState.TASK_KILLED, IContainerDetails.ContainerStatus.DESTROYED);
            put(ExtendedTaskState.TASK_LOST, IContainerDetails.ContainerStatus.DESTROYED);
            put(ExtendedTaskState.TASK_LOST_WHILE_DOWN, IContainerDetails.ContainerStatus.DESTROYED);
            put(ExtendedTaskState.TASK_ERROR, IContainerDetails.ContainerStatus.ERROR);
            put(ExtendedTaskState.TASK_DROPPED, IContainerDetails.ContainerStatus.ERROR);
            put(ExtendedTaskState.TASK_GONE, IContainerDetails.ContainerStatus.FINISHED);
            put(ExtendedTaskState.TASK_UNREACHABLE, IContainerDetails.ContainerStatus.UNKNOWN);
            put(ExtendedTaskState.TASK_GONE_BY_OPERATOR, IContainerDetails.ContainerStatus.UNKNOWN);
            put(ExtendedTaskState.TASK_UNKNOWN, IContainerDetails.ContainerStatus.UNKNOWN);
        }
    };

    public ISingularityScheduler(String url) {
        List<String> hosts;
        if (url.contains(",")) {
            hosts = Arrays.asList(url.split(","));
        } else {
            hosts = Arrays.asList(url);
        }

        String contextPath = "api";
        HttpConfig config = HttpConfig.newBuilder().build();
        config.getObjectMapper().registerModule(new Jdk8Module());
        config.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        HttpClient httpClient = new NingHttpClient(config);
        client = new SingularityClient(contextPath, httpClient, hosts, Optional.empty());
    }

    private String fixName(String id) {
        return id.toLowerCase().replaceAll("[^\\w-]", "");
    }

    private String createId(String taskId, Integer deployId) {
        return taskId + ";" + deployId;
    }

    private String taskId(String id) {
        return id.split(";")[0];
    }

    private Integer DeployId(String id) {
        return Integer.parseInt(id.split(";")[1]);
    }

    @Override
    public String createGroup(String name) throws ISchedulerException {
        try {
            String id = fixName(name) + "-" + UUID.randomUUID().toString();
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
    public String createSingleContainer(String group, String name, IContainerDetails container, IProperties props) throws ISchedulerException {
        return createContainerIntances(group, name, container, props, 1).get(0);
    }

    @Override
    public List<String> createContainerIntances(String group, String name, IContainerDetails container, IProperties props, int instances) throws ISchedulerException {
        try {
            String id = fixName(name) + "-" + UUID.randomUUID().toString();
            SingularityRequestBuilder requestBuilder = new SingularityRequestBuilder(id, RequestType.ON_DEMAND);
            requestBuilder.setGroup(Optional.of(group));
            requestBuilder.setInstances(Optional.of(instances));
            requestBuilder.setBounceAfterScale(Optional.of(false));
            requestBuilder.setNumRetriesOnFailure(Optional.of(0));

            client.createOrUpdateSingularityRequest(requestBuilder.build());

            List<SingularityId> tasksId = new ArrayList<>();

            SingularityDeployBuilder deployBuilder = new SingularityDeployBuilder(id, "0");

            List<SingularityVolume> deployVolumes = new ArrayList<>();
            List<SingularityDockerPortMapping> deployPortMappings = new ArrayList<>();
            Map<String, String> deployParameters = new HashMap<>();
            List<SingularityDockerParameter> deployDockerParameters = new ArrayList<>();

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

            deployBuilder.setCommand(Optional.of(container.getCommand()));
            deployBuilder.setArguments(Optional.of(container.getArguments()));

            if (container.getPorts() != null) {
                int i = 0;
                for (IPort port : container.getPorts()) {
                    deployPortMappings.add(new SingularityDockerPortMapping(
                            Optional.of(SingularityPortMappingType.LITERAL),
                            port.getContainerPort(),
                            Optional.of(SingularityPortMappingType.FROM_OFFER),
                            i++,
                            Optional.of(port.getProtocol())
                    ));
                }
            }

            deployBuilder.setResources(Optional.of(new Resources(
                    container.getCpus(), /*cpus*/
                    (double) container.getMemory(), /*memoryMb*/
                    deployPortMappings.size(),/*numPorts*/
                    0/*diskMb*/
            )));

            if (props.contains(IKeys.SCHEDULER_DNS)) {
                List<String> hostnames = props.getStringList(IKeys.SCHEDULER_DNS);
                for (String hostname : hostnames) {
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
                                                put("o=size", vol.getSize() + "m");
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
            if (container.getEnvironmentVariables() != null) {
                deployEnv.putAll(container.getEnvironmentVariables());
            }

            if (Boolean.getBoolean(IKeys.DEBUG)) {
                try {
                    LOGGER.info("Debug: " + new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(deployBuilder.build()));
                } catch (JsonProcessingException e) {
                    LOGGER.info("Debug: " + deployBuilder.build().toString());
                }
            }

            SingularityRequestParent parent = client.createDeployForSingularityRequest(id, deployBuilder.build(), Optional.of(true), Optional.empty());

            parent = client.updateIncrementalDeployInstanceCount(new SingularityUpdatePendingDeployRequest(id,"0",4));

            tasksId.addAll(parent.getTaskIds().get().getHealthy());
            tasksId.addAll(parent.getTaskIds().get().getNotYetHealthy());
            tasksId.addAll(parent.getTaskIds().get().getPending());
            tasksId.addAll(parent.getTaskIds().get().getCleaning());
            tasksId.addAll(parent.getTaskIds().get().getLoadBalanced());
            tasksId.addAll(parent.getTaskIds().get().getKilled());

            tasksId = tasksId.subList(0, instances);
            synchronized (this) {
                var requestGroup = client.getRequestGroup(group).get();
                requestGroup.getRequestIds().add(id);
                client.saveRequestGroup(requestGroup);
            }
            return tasksId.stream().map(ti -> createId(ti.getId(), 0)).collect(Collectors.toList());
        } catch (SingularityClientException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IContainerDetails.ContainerStatus getStatus(String id) throws ISchedulerException {
        var state = client.getTaskState(taskId(id));
        if (state.isPresent() && state.get().getCurrentState().isPresent()) {
            TASK_STATUS.getOrDefault(state.get().getCurrentState().get(), IContainerDetails.ContainerStatus.UNKNOWN);
        }
        return IContainerDetails.ContainerStatus.UNKNOWN;
    }

    @Override
    public IContainerDetails getContainer(String id) throws ISchedulerException {
        return getContainerInstances(Arrays.asList(id)).get(0);
    }

    @Override
    public List<IContainerDetails> getContainerInstances(List<String> ids) throws ISchedulerException {
        return null;
    }

    @Override
    public IContainerDetails restartContainer(String id) throws ISchedulerException {
        IContainerDetails old = getContainer(id);

        return null;
    }

    @Override
    public void destroyContainer(String id) throws ISchedulerException {

    }

    @Override
    public void destroyContainerInstaces(List<String> ids) throws ISchedulerException {

    }

    @Override
    public void healthCheck() throws ISchedulerException {
        try {
            client.getState(Optional.of(true), Optional.of(false));
        } catch (SingularityClientException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }
}
