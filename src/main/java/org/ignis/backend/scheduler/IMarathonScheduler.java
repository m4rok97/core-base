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
package org.ignis.backend.scheduler;

import com.google.gson.Gson;

import java.lang.reflect.Field;
import java.util.*;

import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.MarathonClient;
import mesosphere.marathon.client.MarathonException;
import mesosphere.marathon.client.model.v2.*;
import org.ignis.backend.exception.ISchedulerException;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.scheduler.model.IBind;
import org.ignis.backend.scheduler.model.IContainerDetails;
import org.ignis.backend.scheduler.model.IPort;
import org.ignis.backend.scheduler.model.IVolume;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public class IMarathonScheduler implements IScheduler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IMarathonScheduler.class);

    private final Marathon marathon;
    private final static Map<String, IContainerDetails.ContainerStatus> TASK_STATUS = new HashMap<>() {
        {
            put("TASK_DROPPED", IContainerDetails.ContainerStatus.ERROR);
            put("TASK_ERROR", IContainerDetails.ContainerStatus.ERROR);
            put("TASK_FAILED", IContainerDetails.ContainerStatus.ERROR);
            put("TASK_FINISHED", IContainerDetails.ContainerStatus.FINISHED);
            put("TASK_GONE", IContainerDetails.ContainerStatus.FINISHED);
            put("TASK_GONE_BY_OPERATOR", IContainerDetails.ContainerStatus.UNKNOWN);
            put("TASK_KILLED", IContainerDetails.ContainerStatus.DESTROYED);
            put("TASK_KILLING", IContainerDetails.ContainerStatus.DESTROYED);
            put("TASK_LOST", IContainerDetails.ContainerStatus.DESTROYED);
            put("TASK_RUNNING", IContainerDetails.ContainerStatus.RUNNING);
            put("TASK_STAGING", IContainerDetails.ContainerStatus.ACCEPTED);
            put("TASK_STARTING", IContainerDetails.ContainerStatus.ACCEPTED);
            put("TASK_UNKNOWN", IContainerDetails.ContainerStatus.UNKNOWN);
            put("TASK_UNREACHABLE", IContainerDetails.ContainerStatus.UNKNOWN);
        }
    };

    public IMarathonScheduler(String url) {
        if (url.contains(",")) {
            for (String url2 : url.split(",")) {
                url = url2;
                try {
                    Marathon candidate = MarathonClient.getInstance(url);
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

    private String appId(String taskId) {
        if (taskId.startsWith("/")) {
            return taskId;
        }
        return "/" + taskId.split("\\.")[0].replace('_', '/');
    }

    @SuppressWarnings("unchecked")
    private App createApp(String group, String name, IContainerDetails container, IProperties props) {
        App app = new App();
        app.setArgs(new ArrayList<>());
        app.setContainer(new Container());
        app.getContainer().setDocker(new Docker());
        app.setConstraints(new ArrayList<>());
        app.getContainer().setVolumes(new ArrayList<>());
        app.getContainer().setPortMappings(new ArrayList<>());

        name = fixMarathonId(name);
        if (group != null) {
            app.setId("/" + group + "/" + name + "-" + UUID.randomUUID().toString());
        } else {
            app.setId("/" + name + "-" + UUID.randomUUID().toString());
        }
        app.getContainer().getDocker().setImage(container.getImage());
        app.getContainer().getDocker().setNetwork("BRIDGE");
        app.getContainer().getDocker().setParameters(new ArrayList<>());
        app.setCpus((double) container.getCpus());
        app.setMem((double) container.getMemory());
        app.getArgs().add(container.getCommand());

        if (container.getSwappiness() != null) {
            app.getContainer().getDocker().getParameters().add(new Parameter("memory-swappiness", "" + container.getSwappiness()));
            app.addLabel("memory-swappiness", "" + container.getSwappiness());
        }

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

        if (props.contains(IKeys.SCHEDULER_DNS)) {
            List<String> hostnames = props.getStringList(IKeys.SCHEDULER_DNS);
            for (String hostname : hostnames) {
                app.getContainer().getDocker().getParameters().add(new Parameter("add-host", hostname));
            }
        }

        if (props.contains(IKeys.SCHEDULER_CONTAINER)) {
            app.getContainer().setType(props.getString(IKeys.SCHEDULER_CONTAINER));
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
                String json = "{persistent:{size: " + vol.getSize() + "}}";//Fix access bug
                PersistentLocalVolume vol2 = new Gson().fromJson(json, PersistentLocalVolume.class);
                vol2.setMode("RW");
                vol2.setContainerPath(vol.getContainerPath());
                app.getContainer().getVolumes().add(vol2);
            }
        }

        if (container.getPreferedHosts() != null) {
            app.getConstraints().add(Arrays.asList("hostname", "LIKE", String.join("|", container.getPreferedHosts())));
        }

        app.setEnv(new HashMap<>());
        app.getEnv().put("IGNIS_JOB_NAME", app.getId());
        app.getEnv().put("IGNIS_JOB_GROUP", group);
        if (System.getenv("TZ") != null) { //Copy timezone
            app.getEnv().put("TZ", System.getenv("TZ"));
        }
        if (container.getEnvironmentVariables() != null) {
            app.getEnv().putAll(container.getEnvironmentVariables());
        }

        app.setMaxLaunchDelaySeconds(21474835); //Max value, no relaunch
        app.setBackoffFactor(app.getMaxLaunchDelaySeconds().doubleValue());
        app.setBackoffSeconds(app.getMaxLaunchDelaySeconds());

        if (Boolean.getBoolean(IKeys.DEBUG)) {
            LOGGER.info("Debug: " + app.toString());
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
    private IContainerDetails parseTaks(String id, App app, Task task) {
        IContainerDetails.IContainerDetailsBuilder builder = IContainerDetails.builder();
        builder.id(id);
        builder.host(task.getHost());
        builder.image(app.getContainer().getDocker().getImage());
        builder.cpus(app.getCpus().intValue());
        builder.memory(app.getMem().longValue());

        if (app.getArgs() != null && !app.getArgs().isEmpty()) {
            builder.command(app.getArgs().get(0));
            builder.arguments(app.getArgs().subList(1, app.getArgs().size()));
        }

        if (app.getLabels().containsKey("memory-swappiness")) {
            builder.swappiness(Integer.parseInt(app.getLabels().get("memory-swappiness")));
        }

        if (app.getContainer() != null) {

            if (app.getContainer().getPortMappings() != null) {
                List<IPort> ports = new ArrayList<>();
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
                                .size(sz)
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

        if (app.getConstraints() != null) {
            for (List<String> constraint : app.getConstraints()) {
                if (constraint.size() == 3 && constraint.get(0).equals("hostname") && constraint.get(1).equals("LIKE")) {
                    builder.preferedHosts(Arrays.asList(constraint.get(2).split("|")));
                    break;
                }
            }
        }

        if (app.getEnv() != null) {
            builder.environmentVariables((Map) app.getEnv());
        }

        return builder.build();
    }

    @Override
    public String createGroup(String name) throws ISchedulerException {
        try {
            String id = fixMarathonId(name) + "-" + UUID.randomUUID().toString();
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
    public String createSingleContainer(String group, String name, IContainerDetails container, IProperties props) throws ISchedulerException {
        try {
            App app = createApp(group, name, container, props);
            app.setInstances(1);
            return marathon.createApp(app).getId();
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<String> createContainerIntances(String group, String name, IContainerDetails container, IProperties props, int instances) throws ISchedulerException {
        try {
            App app = createApp(group, name, container, props);
            app.setInstances(instances);
            boolean first = false;
            int taks = 0;

            app = marathon.createApp(app);
            int time = 1;
            while (!app.getDeployments().isEmpty()) {
                app = marathon.getApp(app.getId()).getApp();
                LOGGER.info("Waiting cluster deployment..." + app.getTasks().size() + " of " + instances);
                Thread.sleep(time * 1000);

                if (first || taks > app.getTasks().size() || taks == instances) {
                    taks = app.getTasks().size();
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
                if (first = dpFirst.equals(dpId)) {
                    continue;
                }

                marathon.deleteApp(app.getId());
                Thread.sleep(10000);
                time = 1;
                app = null;
                while (app != null) {
                    try {
                        app = marathon.createApp(app);
                    } catch (MarathonException response) {
                        if (response.getStatus() != 409) {// App is not destroyed yet
                            throw response;
                        }
                        Thread.sleep(10000);
                    }
                }
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
    public IContainerDetails.ContainerStatus getStatus(String id) throws ISchedulerException {
        try {
            List<Task> tasks = marathon.getAppTasks(appId(id)).getTasks();
            try {
                return TASK_STATUS.get(getTask(tasks, id).getState());
            } catch (ISchedulerException ex) {
                return IContainerDetails.ContainerStatus.DESTROYED;
            }
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }

    @Override
    public IContainerDetails getContainer(String id) throws ISchedulerException {
        return getContainerInstances(Arrays.asList(id)).get(0);
    }

    /**
     * @param ids
     * @return
     * @throws ISchedulerException
     */
    @Override
    public List<IContainerDetails> getContainerInstances(List<String> ids) throws ISchedulerException {
        if (ids.isEmpty()) {
            return new ArrayList<>();
        }
        String appId = appId(ids.get(0));
        for (String id : ids) {
            if (!appId.equals(appId(id))) {
                throw new ISchedulerException("Instances must belong to the same container");
            }
        }
        try {
            App app;

            while ((app = marathon.getApp(appId).getApp()) != null && !app.getDeployments().isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
            }
            if (Boolean.getBoolean(IKeys.DEBUG)) {
                LOGGER.info("Debug: " + app.toString());
            }

            List<IContainerDetails> containers = new ArrayList<>();
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
    public IContainerDetails restartContainer(String id) throws ISchedulerException {
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
    public void destroyContainer(String id) throws ISchedulerException {
        destroyContainerInstaces(Arrays.asList(id));
    }

    @Override
    public void destroyContainerInstaces(List<String> ids) throws ISchedulerException {
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
}
