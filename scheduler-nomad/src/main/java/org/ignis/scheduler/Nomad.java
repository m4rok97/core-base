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

import com.hashicorp.nomad.apimodel.*;
import com.hashicorp.nomad.javasdk.*;
import org.ignis.scheduler.model.*;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author César Pomar
 * <p>
 * Custom scheduler parameters:
 * nomad.frequency=(int)           : Base CPU frequency for requested core unit. By default a homogeneous cluster is assumed.
 * nomad.frequency.lock=(boolean)  : If true (default), only Nodes with more than ${nomad.frequency} can be selected.
 * nomad.devices=(json)            : Must be a json array and will be place in (job -> group -> task -> resources -> device)
 * nomad.datacenters=(String)      : Comma separated list of nomad datacenters, default ignis
 * nomad.shm_size=(int)            : Request a custom value of shared memory
 * nomad.attemps=(int)             : Number of restarts before rescheduling the an Executor container, default 3
 */
public class Nomad implements IScheduler {

    public static final String NAME = "nomad";
    private static final String MODIFIED = "modifyIndex";
    private static final String INSTANCES = "instances";
    private final static Map<String, IContainerStatus> TASK_STATUS = new HashMap<>() {
        {
            put("pending", IContainerStatus.ACCEPTED);
            put("running", IContainerStatus.RUNNING);
            put("complete", IContainerStatus.FINISHED);
            put("failed", IContainerStatus.ERROR);
            put("lost", IContainerStatus.DESTROYED);
        }
    };

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Nomad.class);

    private final NomadApiClient client;
    private final Integer cpuFrequencyDefault;

    public Nomad(String url) {
        NomadApiConfiguration.Builder configBuilder = new NomadApiConfiguration.Builder();

        if (url.contains(",")) {
            for (String url2 : url.split(",")) {
                url = url2;
                try {
                    configBuilder.setAddress(url);
                    new NomadApiClient(configBuilder.build()).getAgentApi().health();
                    break;
                } catch (Exception ex) {
                    LOGGER.warn(url + "Connection refused");
                }

            }
        } else {
            configBuilder.setAddress(url);
        }
        client = new NomadApiClient(configBuilder.build());
        Integer cpuFrequencyDefault = null;
        try {
            String id = client.getNodesApi().list().getValue().get(0).getId();
            Map<String, String> attr = client.getNodesApi().info(id).getValue().getAttributes();
            cpuFrequencyDefault = Integer.parseInt(attr.get("cpu.frequency"));
        } catch (Exception e) {
        }
        this.cpuFrequencyDefault = cpuFrequencyDefault;
    }

    private String newId() {
        UUID id = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 + 1);
        buffer.put((byte) 0);
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        return new BigInteger(buffer.array()).toString(36);
    }

    private void configureResources(Task task, IContainerInfo container) throws ISchedulerException {
        Map<String, String> params = container.getSchedulerParams();
        if (params == null) {
            params = new HashMap<>();
        }
        Resources res = new Resources();
        task.getMeta().put("cores", "" + container.getCpus());
        task.getMeta().put("memory", "" + container.getMemory());
        res.setMemoryMb((int) (container.getMemory() / (1024 * 1024)));
        res.addUnmappedProperty("memory_max", (int) container.getMemory());
        Integer freq;
        if (params.containsKey("nomad.frequency")) {
            freq = Integer.parseInt(params.get("nomad.frequency"));
        } else {
            freq = cpuFrequencyDefault;
        }
        if (freq == null) {
            throw new ISchedulerException("Failed to detect frequency of node, check if cpu.frequency is available " +
                    "as node attribute or define nomad.frequency to set manually.");
        }
        res.setCpu(container.getCpus() * freq);
        if (!params.containsKey("nomad.frequency.lock") || params.get("nomad.frequency.lock").equals("true")) {
            Constraint constraint = new Constraint();
            constraint.setLTarget("${attr.cpu.frequency}");
            constraint.setOperand(">=");
            constraint.setRTarget("" + freq);
            task.getConstraints().add(constraint);
        }

        /*Nomad devices can be maped from user json*/
        if (params.containsKey("nomad.devices")) {
            res.addUnmappedProperty("devices", params.get("nomad.devices"));
        }


        task.setResources(res);
    }

    List<String> getDatacenters(IContainerInfo info) {
        List<String> datacenters = new ArrayList<>();
        if (info.getSchedulerParams() != null && info.getSchedulerParams().containsKey("nomad.datacenters")) {
            datacenters.addAll(Arrays.asList(info.getSchedulerParams().get("").split(",")));
        }
        datacenters.add("ignis");
        return datacenters;
    }

    private String fixName(String name) {
        return name.toLowerCase().replaceAll("[^\\w-\\\\]", "");
    }

    private TaskGroup parseContainer(String name, IContainerInfo container, int instances, boolean driver) throws ISchedulerException {
        Map<String, String> params = container.getSchedulerParams();
        if (params == null) {
            params = new HashMap<>();
        }
        TaskGroup taskGroup = new TaskGroup();
        taskGroup.setName(name);
        taskGroup.setCount(instances);
        taskGroup.setNetworks(new ArrayList<>());
        taskGroup.setMeta(new HashMap<>());
        taskGroup.getMeta().put(INSTANCES, "" + instances);
        Task task = new Task();
        task.setMeta(new HashMap<>());
        task.setConstraints(new ArrayList<>());
        task.setAffinities(new ArrayList<>());
        task.setEnv(new HashMap<>());
        configureResources(task, container);
        if (driver) {
            task.setName("Driver");
        } else {
            task.setName("Executor");
        }
        task.setDriver("docker");
        Map<String, Object> config = new HashMap<>();
        config.put("cpu_hard_limit", true);
        config.put("image", container.getImage());
        config.put("command", container.getCommand());
        if (container.getArguments() != null) {
            config.put("args", container.getArguments());
        }
        if (container.getHostnames() != null) {
            config.put("extra_hosts", container.getHostnames());
        }
        config.put("network_mode", "bridge");

        if (container.getSwappiness() != null) {
            LOGGER.warn("swappiness > 0 ignored. According to the nomad documentation docker containers cannot use swap.");
        }

        if (params.containsKey("nomad.shm_size")) {
            config.put("shm_size", Integer.parseInt(params.get("nomad.shm_size")));
        }

        NetworkResource network = new NetworkResource();
        network.setDynamicPorts(new ArrayList<>());
        network.setReservedPorts(new ArrayList<>());
        List<String> portNames = new ArrayList<>();
        for (IPort port : container.getPorts()) {
            Port port2 = new Port();
            port2.setLabel("port" + portNames.size());
            portNames.add(port2.getLabel());
            if (port.getContainerPort() != 0) {
                port2.setTo(port.getContainerPort());
            }
            if (port.getHostPort() != 0) {
                port2.setValue(port.getHostPort());
                network.getReservedPorts().add(port2);
            } else {
                network.getDynamicPorts().add(port2);
            }
        }
        config.put("ports", portNames);
        taskGroup.getNetworks().add(network);

        taskGroup.setVolumes(new HashMap<>());
        task.setVolumeMounts(new ArrayList<>());
        if (container.getBinds() != null) {
            for (IBind bind : container.getBinds()) {
                VolumeRequest request = new VolumeRequest();
                request.setType("host");
                //request.setName(bind.getHostPath());
                request.setSource(bind.getHostPath());
                request.setReadOnly(bind.isReadOnly());
                taskGroup.getVolumes().put(bind.getHostPath(), request);
                VolumeMount mount = new VolumeMount();
                mount.setVolume(bind.getHostPath());
                mount.setDestination(bind.getContainerPath());
                mount.setReadOnly(bind.isReadOnly());
                task.getVolumeMounts().add(mount);
            }
        }

        List<Map<String, Object>> binds = new ArrayList<>();
        if (container.getVolumes() != null) {
            for (IVolume volume : container.getVolumes()) {
                Map<String, Object> mount = new HashMap<>();
                mount.put("type", "volume");
                mount.put("target", volume.getContainerPath());
                Map<String, Object> driverConfig = new HashMap<>();
                driverConfig.put("name", "local");
                mount.put("driver_config", driverConfig);
                Map<String, Object> driverOptions = new HashMap<>();
                driverOptions.put("size", volume.getSize());
                driverConfig.put("options", driverOptions);

                binds.add(mount);
            }
        }
        config.put("mounts", binds);

        if (container.getPreferedHosts() != null) {
            Affinity affinity = new Affinity();
            affinity.setRTarget("${attr.unique.hostname}");
            affinity.setOperand("contains");
            affinity.setLTarget(String.join(",", container.getPreferedHosts()));
            taskGroup.getMeta().put("prefered-hosts", affinity.getLTarget());
            affinity.setWeight((short) 100);
            taskGroup.getAffinities().add(affinity);
        }

        if (container.getHostnames() != null) {
            config.put("extra_hosts", container.getHostnames());
        }

        if (container.getEnvironmentVariables() != null) {
            task.getEnv().putAll(container.getEnvironmentVariables());
        }

        RestartPolicy restartPolicy = new RestartPolicy();
        if (driver) {
            restartPolicy.setAttempts(0);
        } else if (params.containsKey("nomad.attemps")) {
            restartPolicy.setAttempts(Integer.parseInt(params.get("nomad.attemps")));
        } else {
            restartPolicy.setAttempts(3);
        }
        restartPolicy.setMode("fail");
        taskGroup.setRestartPolicy(restartPolicy);

        ReschedulePolicy reschedulePolicy = new ReschedulePolicy();
        reschedulePolicy.setAttempts(0);
        taskGroup.setReschedulePolicy(reschedulePolicy);

        task.setConfig(config);
        taskGroup.addTasks(task);

        return taskGroup;
    }

    @SuppressWarnings("unchecked")
    private IContainerInfo parseAllocation(Allocation alloc) {
        Resources res = alloc.getResources();
        NetworkResource netwotk = res.getNetworks().get(0);
        TaskGroup taskGroup = alloc.getJob().getTaskGroups().stream()
                .filter((tg) -> tg.getName().equals(alloc.getTaskGroup())).collect(Collectors.toList()).get(0);
        Task task = taskGroup.getTasks().get(0);
        Map<String, Object> config = task.getConfig();

        IContainerInfo.IContainerInfoBuilder builder = IContainerInfo.builder();

        builder.id(alloc.getName());
        builder.cpus(Integer.parseInt(task.getMeta().get("cores")));
        builder.memory(Long.parseLong(task.getMeta().get("memory")));
        builder.swappiness(0);
        builder.image((String) config.get("image"));
        builder.command((String) config.get("command"));
        if (config.containsKey("arguments")) {
            builder.arguments((List<String>) config.get("arguments"));
        }
        List<String> extra_hosts = null;
        if (config.containsKey("extra_hosts")) {
            extra_hosts = (List<String>) config.get("extra_hosts");
            builder.hostnames(extra_hosts);
        }
        if (extra_hosts != null && extra_hosts.contains(alloc.getNodeName())) {
            builder.host(alloc.getNodeName());
        } else {
            builder.host(netwotk.getIp());
        }
        if (taskGroup.getMeta().containsKey("prefered-hosts")) {
            builder.preferedHosts(Arrays.asList(taskGroup.getMeta().get("prefered-hosts").split(",")));
        }

        Map<String, Port> portMap = new HashMap<>();
        List<IPort> ports = new ArrayList<>();
        if (netwotk.getReservedPorts() != null) {
            for (Port p : netwotk.getReservedPorts()) {
                portMap.put(p.getLabel(), p);
            }
        }
        if (netwotk.getDynamicPorts() != null) {
            for (Port p : netwotk.getDynamicPorts()) {
                portMap.put(p.getLabel(), p);
            }
        }
        for (int i = 0; i < portMap.size(); i++) {
            Port port = portMap.get("port" + i);
            int container = port.getTo() != 0 ? port.getTo() : port.getValue();
            ports.add(new IPort(container, port.getValue(), "tcp"));
            ports.add(new IPort(container, port.getValue(), "udp"));
        }
        builder.networkMode(INetworkMode.BRIDGE);
        builder.ports(ports);
        List<IBind> binds = new ArrayList<>();
        List<IVolume> volumes = new ArrayList<>();
        if (config.get("mounts") != null) {
            List<Map<String, Object>> mounts = (List<Map<String, Object>>) config.get("mounts");
            for (Map<String, Object> mount : mounts) {
                if (mount.get("type").equals("bind")) {
                    binds.add(new IBind(
                            (String) mount.get("source"),
                            (String) mount.get("target"),
                            (boolean) mount.get("readonly")
                    ));
                } else if (mount.get("type").equals("volume")) {
                    Map<String, Object> driverConfig = (Map<String, Object>) mount.get("driver_config");
                    Map<String, Object> driverOptions = (Map<String, Object>) driverConfig.get("options");
                    volumes.add(new IVolume(
                            (String) mount.get("target"),
                            Long.parseLong((String) driverConfig.get("size"))
                    ));
                }
            }
        }
        builder.binds(binds);
        builder.volumes(volumes);
        builder.environmentVariables(task.getEnv());
        builder.schedulerParams(new HashMap<>());

        return builder.build();
    }

    private ContainerId getJobFromInstances(List<String> ids) {
        if (ids.isEmpty()) {
            return null;
        }
        ContainerId reference = new ContainerId(ids.get(0));

        for (String id : ids) {
            ContainerId info = new ContainerId(id);
            if (!info.getJobId().equals(reference.getJobId())) {
                throw new ISchedulerException("Instances must belong to the same job");
            }
            if (!info.getTaskGroup().equals(reference.getTaskGroup())) {
                throw new ISchedulerException("Instances must belong to the same taskGroup");
            }
        }
        return reference;
    }

    private Map<String, AllocationListStub> getAllocations(String jobId, List<String> ids, BigInteger modifyIndex,
                                                           boolean wait, boolean running)
            throws NomadException, IOException {
        QueryOptions<List<AllocationListStub>> options = null;
        Map<String, AllocationListStub> map;
        RETRY:
        while (true) {
            ServerQueryResponse<List<AllocationListStub>> res = client.getJobsApi().allocations(jobId, options);
            options = new QueryOptions<>();
            options.setWaitStrategy(WaitStrategy.WAIT_INDEFINITELY);
            options.setIndex(res.getIndex());
            List<AllocationListStub> allocs = res.getValue();
            Set<String> allowed = new HashSet<>(ids);
            map = allocs.stream()
                    .filter((a) -> {
                        if (!allowed.contains(a.getName())) {
                            return false;
                        }
                        if (a.getCreateIndex().compareTo(modifyIndex) < 0) {
                            return a.getClientStatus().equals("running") || a.getClientStatus().equals("pending");
                        }
                        return true;
                    }).collect(Collectors.toMap(AllocationListStub::getName, Function.identity()));
            if (wait) {
                if (allocs.size() < ids.size()) {
                    continue;
                }
                if (running) {
                    for (String id : ids) {
                        AllocationListStub alloc = map.get(id);
                        if (alloc.getClientStatus().equals("running")) {
                            continue;
                        } else if (alloc.getClientStatus().equals("pending")) {
                            continue RETRY;
                        } else {
                            throw new ISchedulerException(alloc.getClientDescription());
                        }
                    }
                }
            }
            break;
        }
        return map;
    }

    public BigInteger getJobModifyIndex(Job job, String taskGroup) {
        for (TaskGroup tg : job.getTaskGroups()) {
            if (tg.getName().equals(taskGroup)) {
                if (tg.getMeta().containsKey(MODIFIED)) {
                    return new BigInteger(tg.getMeta().get(MODIFIED));
                }
                break;
            }
        }
        return BigInteger.ZERO;
    }

    @Override
    public String createGroup(String name) throws ISchedulerException {
        return new GroupId(newId(), fixName(name)).toString();
    }

    @Override
    public void destroyGroup(String group) throws ISchedulerException {
    }

    @Override
    public String createDriverContainer(String group, String name, IContainerInfo container) throws ISchedulerException {
        try {
            GroupId groupId = new GroupId(group);
            Job job = new Job();
            job.setId(groupId.getJobId());
            job.setName(groupId.getName());
            job.setType("batch");
            job.setDatacenters(getDatacenters(container));
            TaskGroup driver = parseContainer(fixName(name), container, 1, true);
            String driverId = new ContainerId(job.getId(), driver.getName(), 0).toString();
            driver.getTasks().get(0).getEnv().put("IGNIS_JOB_ID", driverId);
            driver.getTasks().get(0).getEnv().put("IGNIS_JOB_NAME", group);
            driver.getMeta().put(MODIFIED, "0");
            job.addTaskGroups(driver);
            client.getJobsApi().register(job);
            return driverId;
        } catch (IOException | NomadException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<String> createExecutorContainers(String group, String name, IContainerInfo container, int instances) throws ISchedulerException {
        try {
            GroupId groupId = new GroupId(group);
            TaskGroup executors = parseContainer(fixName(name), container, instances, false);
            Job job;
            while (true) {
                job = client.getJobsApi().info(groupId.getJobId()).getValue();
                try {
                    boolean set = false;
                    for (int i = 0; i < job.getTaskGroups().size(); i++) {
                        TaskGroup tg = job.getTaskGroups().get(i);
                        if (executors.getName().equals(tg.getName())) {
                            set = true;
                            job.getTaskGroups().set(i, executors);
                            break;
                        }
                    }
                    for (TaskGroup tg : job.getTaskGroups()) {
                        if (executors.getName().equals(tg.getName())) {
                            set = true;
                            break;
                        }
                    }
                    if (!set) {
                        job.addTaskGroups(executors);
                    }
                    executors.getMeta().put(MODIFIED, job.getJobModifyIndex().toString());
                    client.getJobsApi().register(job, job.getJobModifyIndex());
                } catch (NomadException ex0) {
                    if (ex0.getMessage().contains(job.getJobModifyIndex() + ":")
                    ) {
                        continue;
                    }
                    throw ex0;
                }
                break;
            }

            List<String> ids = new ArrayList<>();
            for (int i = 0; i < instances; i++) {
                ids.add(new ContainerId(job.getId(), executors.getName(), i).toString());
            }
            return ids;
        } catch (IOException | NomadException ex) {
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
            ContainerId idInfo = getJobFromInstances(ids);
            if (idInfo == null) {
                return List.of();
            }
            String jobId = idInfo.getJobId();
            Job job = client.getJobsApi().info(jobId).getValue();
            BigInteger modifyIndex = getJobModifyIndex(job, new ContainerId(ids.get(0)).getTaskGroup());

            Map<String, AllocationListStub> map = getAllocations(jobId, ids, modifyIndex, false, false);
            List<IContainerStatus> result = new ArrayList<>();
            for (String id : ids) {
                if (map.containsKey(id)) {
                    result.add(TASK_STATUS.getOrDefault(map.get(id).getClientStatus(), IContainerStatus.UNKNOWN));
                } else {
                    result.add(IContainerStatus.ACCEPTED);
                }
            }
            return result;
        } catch (IOException | NomadException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IContainerInfo getDriverContainer(String id) throws ISchedulerException {
        return getExecutorContainers(List.of(id)).get(0);
    }

    @Override
    public List<IContainerInfo> getExecutorContainers(List<String> ids) throws ISchedulerException {
        try {
            ContainerId idInfo = getJobFromInstances(ids);
            if (idInfo == null) {
                return List.of();
            }
            String jobId = idInfo.getJobId();
            Job job = client.getJobsApi().info(jobId).getValue();
            BigInteger modifyIndex = getJobModifyIndex(job, new ContainerId(ids.get(0)).getTaskGroup());

            Map<String, AllocationListStub> map = getAllocations(jobId, ids, modifyIndex, true, true);
            List<IContainerInfo> result = new ArrayList<>();
            for (String id : ids) {
                result.add(parseAllocation(client.getAllocationsApi().info(map.get(id).getId()).getValue()));
            }
            return result;
        } catch (IOException | NomadException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public synchronized IContainerInfo restartContainer(String id) throws ISchedulerException {
        try {
            IContainerStatus status = getStatus(id);
            if (status == IContainerStatus.ACCEPTED || status == IContainerStatus.RUNNING) {
                return getDriverContainer(id);
            }
            ContainerId containerId = new ContainerId(id);

            for (int i = 0; i < 2; i++) {
                while (true) {
                    Job job = client.getJobsApi().info(containerId.getJobId()).getValue();
                    TaskGroup taskGroup = null;
                    for (TaskGroup tg : job.getTaskGroups()) {
                        if (tg.getName().equals(containerId.getTaskGroup())) {
                            taskGroup = tg;
                            break;
                        }
                    }
                    if (i == 0) {
                        long live = client.getJobsApi().allocations(job.getId()).getValue().stream().filter(a -> {
                            return a.getClientStatus().equals("running") || a.getClientStatus().equals("pending");
                        }).count();
                        taskGroup.setCount((int) live);
                        taskGroup.getMeta().put(MODIFIED, job.getJobModifyIndex().toString());
                    } else {
                        taskGroup.setCount(Integer.parseInt(taskGroup.getMeta().get(INSTANCES)));
                    }
                    try {
                        client.getJobsApi().register(job, job.getJobModifyIndex());
                    } catch (NomadException ex0) {
                        if (ex0.getMessage().contains(job.getJobModifyIndex() + ":")) {
                            continue;
                        }
                        throw ex0;
                    }
                    break;
                }
            }
            return getDriverContainer(id);
        } catch (IOException | NomadException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroyDriverContainer(String id) throws ISchedulerException {
        try {
            client.getJobsApi().deregister(new ContainerId(id).getJobId());
        } catch (IOException | NomadException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroyExecutorInstances(List<String> ids) throws ISchedulerException {
        try {
            ContainerId idInfo = getJobFromInstances(ids);
            if (idInfo == null) {
                return;
            }
            String jobId = idInfo.getJobId();
            String taskGroup = idInfo.getTaskGroup();
            Job job;
            while (true) {
                job = client.getJobsApi().info(jobId).getValue();
                for (TaskGroup task : job.getTaskGroups()) {
                    if (task.getName().equals(taskGroup)) {
                        task.setCount(0);
                    }
                    break;
                }
                try {
                    client.getJobsApi().register(job, job.getJobModifyIndex());
                } catch (NomadException ex0) {
                    if (ex0.getMessage().endsWith("" + job.getJobModifyIndex())) {
                        continue;
                    }
                    throw ex0;
                }
                break;
            }
        } catch (IOException | NomadException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        try {
            client.getAgentApi().health();
        } catch (IOException | NomadException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}
