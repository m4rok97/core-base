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
package org.ignis.scheduler2;

import com.google.gson.Gson;
import mesosphere.marathon.client.MarathonClient;
import mesosphere.marathon.client.MarathonException;
import mesosphere.marathon.client.model.v2.*;
import org.ignis.scheduler2.model.*;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author César Pomar
 */
public class MesosMarathon implements IScheduler {

    public static final String NAME = "mesos_marathon";

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MesosMarathon.class);

    private final mesosphere.marathon.client.Marathon marathon;
    private final static Map<String, IContainerStatus> TASK_STATUS = new HashMap<>() {
        {
            put("TASK_DROPPED", IContainerStatus.ERROR);
            put("TASK_ERROR", IContainerStatus.ERROR);
            put("TASK_FAILED", IContainerStatus.ERROR);
            put("TASK_FINISHED", IContainerStatus.FINISHED);
            put("TASK_GONE", IContainerStatus.FINISHED);
            put("TASK_GONE_BY_OPERATOR", IContainerStatus.UNKNOWN);
            put("TASK_KILLED", IContainerStatus.DESTROYED);
            put("TASK_KILLING", IContainerStatus.DESTROYED);
            put("TASK_LOST", IContainerStatus.DESTROYED);
            put("TASK_RUNNING", IContainerStatus.RUNNING);
            put("TASK_STAGING", IContainerStatus.ACCEPTED);
            put("TASK_STARTING", IContainerStatus.ACCEPTED);
            put("TASK_UNKNOWN", IContainerStatus.UNKNOWN);
            put("TASK_UNREACHABLE", IContainerStatus.UNKNOWN);
        }
    };

    public MesosMarathon(String url) {
        if (url.contains(",")) {
            for (String url2 : url.split(",")) {
                url = url2;
                try {
                    mesosphere.marathon.client.Marathon candidate = MarathonClient.getInstance(url);
                    candidate.getServerInfo();
                    break;
                } catch (Exception ex) {
                    LOGGER.warn(url + "Connection refused");
                }

            }
        }
        marathon = MarathonClient.getInstance(url);
    }

    private String fixMarathonId(String id) {
        return id.toLowerCase().replaceAll("[^\\w-]", "");
    }

    private String newId() {
        UUID id = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 + 1);
        buffer.put((byte) 0);
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        return new BigInteger(buffer.array()).toString(36);
    }

    private String appId(String taskId) {
        if (taskId.startsWith("/")) {
            return taskId;
        }
        return "/" + taskId.split("\\.")[0].replace('_', '/');
    }

    @SuppressWarnings("unchecked")
    private App createApp(String group, String name, IContainerInfo container) {
        App app = new App();
        app.setArgs(new ArrayList<>());
        app.setContainer(new Container());
        app.getContainer().setType("docker");
        app.getContainer().setDocker(new Docker());
        app.setConstraints(new ArrayList<>());
        app.getContainer().setVolumes(new ArrayList<>());
        app.getContainer().setPortMappings(new ArrayList<>());

        name = fixMarathonId(name);
        if (group != null) {
            app.setId("/" + group + "/" + name);
        } else {
            app.setId("/" + name + "-" + newId());
        }
        app.getContainer().getDocker().setImage(container.getImage());
        app.getContainer().getDocker().setNetwork("BRIDGE");
        app.getContainer().getDocker().setParameters(new ArrayList<>());
        app.setCpus((double) container.getCpus());
        app.setMem((double) container.getMemory() / (1000 * 1000));
        app.getArgs().add(container.getCommand());

        if (container.getArguments() != null) {
            app.getArgs().addAll(container.getArguments());
        }

        if (container.getPorts() != null) {
            app.setRequirePorts(true);
            for (IPort port : container.getPorts()) {
                Port port2 = new Port();
                port2.setContainerPort(port.getContainerPort());
                port2.setHostPort(0);
                port2.setProtocol(port.getProtocol());
                app.getContainer().getPortMappings().add(port2);
            }
        }

        if (container.getHostnames() != null) {
            for (String hostname : container.getHostnames()) {
                app.getContainer().getDocker().getParameters().add(new Parameter("add-host", hostname));
            }
        }

        if (container.getBinds() != null) {
            for (IBind bind : container.getBinds()) {
                LocalVolume vol = new LocalVolume();
                vol.setHostPath(bind.getHostPath());
                vol.setContainerPath(bind.getContainerPath());
                vol.setMode(bind.isReadOnly() ? "RO" : "RW");
                app.getContainer().getVolumes().add(vol);
            }
        }

        if (container.getVolumes() != null) {
            for (IVolume vol : container.getVolumes()) {
                String json = "{persistent:{size: " + (vol.getSize() / (1000 * 1000)) + "}}";//Fix access bug
                PersistentLocalVolume vol2 = new Gson().fromJson(json, PersistentLocalVolume.class);
                vol2.setMode("RW");
                vol2.setContainerPath(vol.getContainerPath());
                app.getContainer().getVolumes().add(vol2);
            }
        }

        if (container.getPreferedHosts() != null) {
            app.addLabel("prefered-hosts", container.getPreferedHosts().stream().collect(Collectors.joining(",")));
            app.getConstraints().add(Arrays.asList("hostname", "LIKE", String.join("|", container.getPreferedHosts())));
        }

        app.setEnv(new HashMap<>());
        if (System.getenv("TZ") != null) { //Copy timezone
            app.getEnv().put("TZ", System.getenv("TZ"));
        }
        if (container.getEnvironmentVariables() != null) {
            app.getEnv().putAll(container.getEnvironmentVariables());
        }

        app.setMaxLaunchDelaySeconds(21474835); //Max value, no relaunch
        app.setBackoffFactor(app.getMaxLaunchDelaySeconds().doubleValue());
        app.setBackoffSeconds(app.getMaxLaunchDelaySeconds());

        if (Boolean.getBoolean("ignis.debug")) {
            LOGGER.info("Debug: " + app);
        }

        return app;
    }

    private Task getTask(Collection<Task> tasks, String id) throws ISchedulerException {
        if (id.startsWith("/") && tasks.size() == 1) {
            return tasks.iterator().next();
        }
        for (Task t : tasks) {
            if (t.getId().startsWith(id)) {
                return t;
            }
        }
        throw new ISchedulerException("marathon task not found");
    }

    @SuppressWarnings("unchecked")
    private IContainerInfo parseTaks(String id, App app, Task task) {
        IContainerInfo.IContainerInfoBuilder builder = IContainerInfo.builder();
        builder.id(id);
        builder.host(task.getHost());
        builder.image(app.getContainer().getDocker().getImage());
        builder.cpus(app.getCpus().intValue());
        builder.memory(app.getMem().longValue() * 1000 * 1000);

        if (app.getArgs() != null && !app.getArgs().isEmpty()) {
            builder.command(app.getArgs().get(0));
            builder.arguments(app.getArgs().subList(1, app.getArgs().size()));
        }

        List<String> hostnames = new ArrayList<>();
        builder.hostnames(hostnames);
        for (Parameter param : app.getContainer().getDocker().getParameters()) {
            if (param.getKey().equals("add-host")) {
                hostnames.add(param.getValue());
            }
        }

        if (app.getContainer() != null) {

            if (app.getContainer().getPortMappings() != null) {
                List<IPort> ports = new ArrayList<>();
                builder.networkMode(INetworkMode.BRIDGE);
                builder.ports(ports);
                Iterator<Integer> taskPorts = task.getPorts().iterator();
                for (Port port : app.getContainer().getPortMappings()) {
                    int portHost = taskPorts.next();
                    int portContainer = port.getContainerPort();
                    if (portContainer == 0) {
                        portContainer = portHost;
                    }
                    ports.add(new IPort(portContainer, portHost, port.getProtocol()));
                }
            }

            if (app.getContainer().getVolumes() != null) {
                List<IBind> binds = new ArrayList<>();
                List<IVolume> volumes = new ArrayList<>();

                for (Volume absVol : app.getContainer().getVolumes()) {
                    if (absVol instanceof LocalVolume) {
                        LocalVolume vol = (LocalVolume) absVol;
                        binds.add(IBind.builder().
                                containerPath(vol.getContainerPath()).
                                hostPath(vol.getHostPath()).
                                readOnly(vol.getMode().equals("RO")).
                                build());

                    } else if (absVol instanceof PersistentLocalVolume) {
                        PersistentLocalVolume vol = (PersistentLocalVolume) absVol;
                        Long sz = 0l;
                        try {  //Fix access bug
                            Field field = ((Object) vol.getPersistentLocalVolumeInfo()).getClass().getDeclaredField("size");
                            field.setAccessible(true);
                            sz = (Long) field.get(vol.getPersistentLocalVolumeInfo());
                        } catch (Exception ex) {
                        }
                        volumes.add(IVolume.builder()
                                .containerPath(vol.getContainerPath())
                                .size(sz * 1000 * 1000)
                                .build());
                    }
                }
                if (!binds.isEmpty()) {
                    builder.binds(binds);
                }

                if (!volumes.isEmpty()) {
                    builder.volumes(volumes);
                }
            }
        }

        if(app.getLabels().containsKey("prefered-hosts")){
            builder.preferedHosts(Arrays.asList(app.getLabels().get("prefered-hosts").split(",")));
        }

        if (app.getEnv() != null) {
            builder.environmentVariables((Map) app.getEnv());
        }

        return builder.build();
    }

    private String getAppFromInstances(List<String> ids) {
        if (ids.isEmpty()) {
            return null;
        }
        String appId = appId(ids.get(0));
        for (String id : ids) {
            if (!appId.equals(appId(id))) {
                throw new ISchedulerException("Instances must belong to the same container");
            }
        }
        return appId;
    }

    @Override
    public String createGroup(String name) throws ISchedulerException {
        try {
            String id = fixMarathonId(name) + "-" + newId();
            marathon.createGroup(new Group(id));
            return id;
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroyGroup(String group) throws ISchedulerException {
        try {
            marathon.deleteGroup(group, true);
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public String createDriverContainer(String group, String name, IContainerInfo container) throws ISchedulerException {
        try {
            App app = createApp(group, name, container);
            app.getEnv().put("IGNIS_JOB_ID", app.getId());
            app.getEnv().put("IGNIS_JOB_NAME", group);
            app.setInstances(1);
            return marathon.createApp(app).getId();
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<String> createExecutorContainers(String group, String name, IContainerInfo container, int instances) throws ISchedulerException {
        try {
            App app = createApp(group, name, container);
            app.setInstances(instances);
            boolean first = false;
            int tasks = 0;

            app = marathon.createApp(app);
            long time = 1;
            while (!app.getDeployments().isEmpty() || app.getTasks().size() != instances) {
                app = marathon.getApp(app.getId()).getApp();
                LOGGER.info("Waiting cluster deployment..." + Math.min(app.getTasks().size(), instances) + " of " + instances);
                Thread.sleep(time * 1000);

                if (first || tasks > app.getTasks().size() || tasks == instances) {
                    tasks = app.getTasks().size();
                    continue;
                }

                if (time < 30) {
                    time++;
                    continue;
                }

                String dpId = app.getDeployments().get(0).getId();
                app = marathon.getApp(app.getId()).getApp();
                List<Deployment> deployments = marathon.getDeployments();
                if (deployments.isEmpty()) {
                    continue;
                }

                String dpFirst = deployments.get(0).getId();
                if (first == dpFirst.equals(dpId)) {
                    continue;
                }

                marathon.deleteApp(app.getId());
                Thread.sleep(10000);
                time = 1;
                app = null;
                do {
                    try {
                        app = marathon.createApp(app);
                    } catch (MarathonException response) {
                        if (response.getStatus() != 409) {// App is not destroyed yet
                            throw response;
                        }
                        Thread.sleep(10000);
                    }
                } while (app != null);
            }
            List<String> ids = new ArrayList<>();
            for (Task t : app.getTasks()) {
                String[] fields = t.getId().split("\\.");
                ids.add(fields[0] + "." + fields[1] + ".");
            }
            return ids;
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        } catch (InterruptedException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public String createDriverWithExecutorContainers(String group, String driverName,
                                                     IContainerInfo driverContainer,
                                                     List<ExecutorContainers> executorContainers)
            throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");//TODO
    }

    @Override
    public IContainerStatus getStatus(String id) throws ISchedulerException {
        try {
            String appId = appId(id);
            List<Task> tasks = marathon.getAppTasks(appId).getTasks();
            try {
                return TASK_STATUS.get(getTask(tasks, id).getState());
            } catch (ISchedulerException ex) {
                return IContainerStatus.UNKNOWN;
            }
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<IContainerStatus> getStatus(List<String> ids) throws ISchedulerException {
        String appId = getAppFromInstances(ids);
        if (appId == null) {
            return new ArrayList<>();
        }
        try {
            List<Task> tasks = marathon.getAppTasks(appId).getTasks();
            List<IContainerStatus> status = new ArrayList<>();
            for (String id : ids) {
                try {
                    status.add(TASK_STATUS.get(getTask(tasks, id).getState()));
                } catch (ISchedulerException ex) {
                    status.add(IContainerStatus.UNKNOWN);
                }
            }
            return status;
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IContainerInfo getContainer(String id) throws ISchedulerException {
        return getExecutorContainers(Arrays.asList(id)).get(0);
    }

    /**
     * @param ids
     * @return
     * @throws ISchedulerException
     */
    @Override
    public List<IContainerInfo> getExecutorContainers(List<String> ids) throws ISchedulerException {
        String appId = getAppFromInstances(ids);
        if (appId == null) {
            return new ArrayList<>();
        }
        try {
            App app;

            while ((app = marathon.getApp(appId).getApp()) != null && !app.getDeployments().isEmpty() &&
                    app.getTasks().size() < ids.size()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
            }
            if (Boolean.getBoolean("ignis.debug")) {
                LOGGER.info("Debug: " + app.toString());
            }

            List<IContainerInfo> containers = new ArrayList<>();
            for (String id : ids) {
                Task t = getTask(app.getTasks(), id);
                containers.add(parseTaks(id, app, t));
            }
            return containers;
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IContainerInfo restartContainer(String id) throws ISchedulerException {
        try {
            String appId = appId(id);
            List<Task> tasks = marathon.getAppTasks(appId).getTasks();
            String realId = getTask(tasks, id).getId();
            marathon.deleteAppTask(appId, realId, "false");
            OUT:
            while (tasks != null) {
                tasks = marathon.getAppTasks(appId).getTasks();
                for (Task t : tasks) {
                    if (t.getId().equals(realId)) {
                        continue OUT;
                    }
                }
                break;
            }
            return getContainer(id);
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroyDriverContainer(String id) throws ISchedulerException {
        destroyExecutorInstances(Arrays.asList(id));
    }

    @Override
    public void destroyExecutorInstances(List<String> ids) throws ISchedulerException {
        try {
            if (ids.isEmpty()) {
                return;
            }
            String appId = appId(ids.get(0));
            for (String id : ids) {
                if (!appId.equals(appId(id))) {
                    throw new ISchedulerException("Instances must belong to the same container");
                }
            }
            App app = marathon.getApp(appId).getApp();
            if (app.getTasks().size() == ids.size()) {
                marathon.deleteApp(appId);
            } else {
                MarathonException error = null;
                for (String id : ids) {
                    try {
                        String realId = getTask(app.getTasks(), id).getId();
                        marathon.deleteAppTask(appId, realId, "true");
                    } catch (MarathonException ex0) {
                        error = ex0;
                    }
                }
                if (error != null) {
                    throw error;
                }
            }
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }

    }

    @Override
    public void healthCheck() throws ISchedulerException {
        try {
            marathon.getServerInfo();
        } catch (Exception ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isDynamic() {
        return true;
    }
}
