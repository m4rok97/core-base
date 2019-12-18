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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import mesosphere.marathon.client.Marathon;
import mesosphere.marathon.client.MarathonClient;
import mesosphere.marathon.client.MarathonException;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.Container;
import mesosphere.marathon.client.model.v2.Docker;
import mesosphere.marathon.client.model.v2.Group;
import mesosphere.marathon.client.model.v2.LocalVolume;
import mesosphere.marathon.client.model.v2.PersistentLocalVolume;
import mesosphere.marathon.client.model.v2.Port;
import mesosphere.marathon.client.model.v2.Task;
import mesosphere.marathon.client.model.v2.VersionedApp;
import mesosphere.marathon.client.model.v2.Volume;
import org.ignis.backend.exception.ISchedulerException;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.scheduler.model.IBind;
import org.ignis.backend.scheduler.model.IJobContainer;
import org.ignis.backend.scheduler.model.INetwork;
import org.ignis.backend.scheduler.model.IVolume;

/**
 *
 * @author César Pomar
 */
public class IMarathonScheduler implements IScheduler {
    
    private final Marathon marathon;
    private final Map<String, String> taskAssignment;
    private final Set<String> taskAssigned;
    private final static Map<String, IJobContainer.ContainerStatus> TASK_STATUS
            = new HashMap<String, IJobContainer.ContainerStatus>() {
        {
            put("TASK_DROPPED", IJobContainer.ContainerStatus.ERROR);
            put("TASK_ERROR", IJobContainer.ContainerStatus.ERROR);
            put("TASK_FAILED", IJobContainer.ContainerStatus.ERROR);
            put("TASK_FINISHED", IJobContainer.ContainerStatus.FINISHED);
            put("TASK_GONE", IJobContainer.ContainerStatus.FINISHED);
            put("TASK_GONE_BY_OPERATOR", IJobContainer.ContainerStatus.UNKNOWN);
            put("TASK_KILLED", IJobContainer.ContainerStatus.DESTROYED);
            put("TASK_KILLING", IJobContainer.ContainerStatus.DESTROYED);
            put("TASK_LOST", IJobContainer.ContainerStatus.DESTROYED);
            put("TASK_RUNNING", IJobContainer.ContainerStatus.RUNNING);
            put("TASK_STAGING", IJobContainer.ContainerStatus.ACCEPTED);
            put("TASK_STARTING", IJobContainer.ContainerStatus.ACCEPTED);
            put("TASK_UNKNOWN", IJobContainer.ContainerStatus.UNKNOWN);
            put("TASK_UNREACHABLE", IJobContainer.ContainerStatus.UNKNOWN);
        }
    };
    
    public IMarathonScheduler(String url) {
        marathon = MarathonClient.getInstance(url);
        taskAssignment = new HashMap<>();
        taskAssigned = new HashSet<>();
    }
    
    private App createApp(String group, String name, IJobContainer container, IProperties props) {
        App app = new App();
        app.setArgs(new ArrayList<>());
        app.setContainer(new Container());
        app.getContainer().setDocker(new Docker());
        app.setConstraints(new ArrayList<>());
        app.getContainer().setVolumes(new ArrayList<>());
        app.getContainer().setPortMappings(new ArrayList<>());
        
        if (group != null) {
            app.setId(group + "/" + name + "-" + UUID.randomUUID().toString());
        } else {
            app.setId(name + "-" + UUID.randomUUID().toString());
        }
        app.getContainer().getDocker().setImage(container.getImage());
        app.getContainer().getDocker().setNetwork("BRIDGE");
        app.setCpus((double) container.getCpus());
        app.setMem((double) container.getMemory());
        app.getArgs().add(container.getCommand());
        
        if (container.getArguments() != null) {
            app.getArgs().addAll(container.getArguments());
        }
        
        if (container.getNetwork() != null) {
            app.setRequirePorts(true);
            for (Map.Entry<Integer, Integer> port : container.getNetwork().getTcpMap().entrySet()) {
                Port port2 = new Port();
                port2.setContainerPort(port.getKey());
                port2.setHostPort(port.getValue());
                port2.setProtocol("tcp");
                app.getContainer().getPortMappings().add(port2);
            }
            for (Map.Entry<Integer, Integer> port : container.getNetwork().getUdpMap().entrySet()) {
                Port port2 = new Port();
                port2.setContainerPort(port.getKey());
                port2.setHostPort(port.getValue());
                port2.setProtocol("udp");
                app.getContainer().getPortMappings().add(port2);
            }
            
            for (Integer port : container.getNetwork().getTcpPorts()) {
                Port port2 = new Port();
                port2.setHostPort(0);
                port2.setContainerPort(port);
                port2.setProtocol("tcp");
                app.getContainer().getPortMappings().add(port2);
            }
            
            for (Integer port : container.getNetwork().getUdpPorts()) {
                Port port2 = new Port();
                port2.setHostPort(0);
                port2.setContainerPort(port);
                port2.setProtocol("udp");
                app.getContainer().getPortMappings().add(port2);
            }
        }
        
        if (props.contains(IKeys.SCHEDULER_DNS) && props.getString(IKeys.SCHEDULER_DNS).toLowerCase().equals("host")) {
            LocalVolume vol = new LocalVolume();
            vol.setHostPath("/etc/hosts");
            vol.setContainerPath("/etc/hosts");
            vol.setMode("RO");
            app.getContainer().getVolumes().add(vol);
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
        
        if (container.getEnvironmentVariables() != null) {
            app.setEnv((Map) container.getEnvironmentVariables());
        }
        
        return app;
    }
    
    private String taskId(Task task) throws ISchedulerException {
        return task.getId().split(".")[2];
    }
    
    private Task getTask(App app, String id) throws ISchedulerException {
        for (Task t : app.getTasks()) {
            if (id.equals(taskId(t))) {
                return t;
            }
        }
        throw new ISchedulerException("not found");
    }
    
    private IJobContainer parseTaks(App app, Task task) {
        IJobContainer.IJobContainerBuilder builder = IJobContainer.builder();
        builder.image(app.getContainer().getDocker().getImage());
        builder.cpus(app.getCpus().intValue());
        builder.memory(app.getMem().longValue());
        
        if (app.getArgs() != null && !app.getArgs().isEmpty()) {
            builder.command(app.getArgs().get(0));
            builder.arguments(app.getArgs().subList(1, app.getArgs().size()));
        }
        
        if (app.getContainer() != null) {
            
            if (app.getContainer().getPortMappings() != null) {
                INetwork network = new INetwork();
                builder.network(network);
                Iterator<Integer> ports = task.getPorts().iterator();
                for (Port port : app.getContainer().getPortMappings()) {
                    int portHost = ports.next();
                    int portContainer = port.getContainerPort();
                    if (portContainer == 0) {
                        portContainer = portHost;
                    }
                    if (port.getProtocol().equals("tcp")) {
                        network.getTcpMap().put(portContainer, portHost);
                        network.getTcpPorts().add(port.getContainerPort());
                    } else {
                        network.getUdpMap().put(portContainer, portHost);
                        network.getUdpPorts().add(port.getContainerPort());
                    }
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
            String id = name + "-" + UUID.randomUUID().toString();
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
    public String createSingleContainer(String group, String name, IJobContainer container, IProperties props) throws ISchedulerException {
        return createContainerIntances(group, name, container, props, 1).get(0);
    }
    
    @Override
    public List<String> createContainerIntances(String group, String name, IJobContainer container, IProperties props, int instances) throws ISchedulerException {
        App app = createApp(group, name, container, props);
        app.setInstances(instances);
        try {
            marathon.createApp(app);
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < instances; i++) {
                ids.add(app.getId() + ";" + i);
            }
            return ids;
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }
    
    @Override
    public IJobContainer.ContainerStatus getStatus(String id) throws ISchedulerException {
        String appId = id.split(";")[0];
        try {
            VersionedApp app = marathon.getApp(appId).getApp();
            if (app.getInstances() == 0) {
                return IJobContainer.ContainerStatus.DESTROYED;
            }
            String taskId = taskAssignment.get(id);
            if (taskId == null) {
                getContainer(id);
                taskId = taskAssignment.get(id);
            }
            Task task;
            try {
                task = getTask(app, taskId);
            } catch (ISchedulerException ex) {
                return IJobContainer.ContainerStatus.DESTROYED;
            }
            return TASK_STATUS.get(task.getState());
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }
    
    @Override
    public IJobContainer getContainer(String id) throws ISchedulerException {
        return getContainerInstances(Arrays.asList(id)).get(0);
    }

    /**
     *
     * @param ids
     * @return
     * @throws ISchedulerException
     */
    @Override
    public List<IJobContainer> getContainerInstances(List<String> ids) throws ISchedulerException {
        if (ids.isEmpty()) {
            return new ArrayList<>();
        }
        String appId = ids.get(0).split(";")[0];
        for (String id : ids) {
            if (!appId.equals(id.split(";")[0])) {
                throw new ISchedulerException("Instances must belong to the same container");
            }
        }
        try {
            List<IJobContainer> containers = new ArrayList<>(ids.size());
            VersionedApp app;
            int time = 1;
            int i = 0;
            do {
                app = marathon.getApp(appId).getApp();
                for (Task t : app.getTasks()) {
                    String taskId = taskId(t);
                    if (taskAssigned.contains(taskId)) {
                        continue;
                    } else if (containers.size() == ids.size()) {
                        break;
                    }
                    taskAssigned.add(taskId);
                    taskAssignment.put(ids.get(i), taskId);
                    containers.add(parseTaks(app, t));
                }
                Thread.sleep(time * 1000);
                if (time < 30) {
                    time++;
                }
            } while (containers.size() == ids.size());
            return containers;
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        } catch (InterruptedException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
        
    }
    
    @Override
    public void restartContainer(String id) throws ISchedulerException {
        String appId = id.split(";")[0];
        String taskId = taskAssignment.get(id);
        try {
            VersionedApp app = marathon.getApp(appId).getApp();
            marathon.deleteAppTask(appId, getTask(app, taskId).getId(), "false");
        } catch (MarathonException ex) {
            throw new ISchedulerException(ex.getMessage(), ex);
        }
    }
    
    @Override
    public void destroyContainer(String id) throws ISchedulerException {
        String appId = id.split(";")[0];
        String taskId = taskAssignment.get(id);
        try {
            VersionedApp app = marathon.getApp(appId).getApp();
            marathon.deleteAppTask(appId, getTask(app, taskId).getId(), "true");
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
