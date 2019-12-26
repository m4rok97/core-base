/*
 * Copyright (C) 2018 
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
package org.ignis.backend.cluster.tasks.container;

import java.util.ArrayList;
import java.util.List;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.scheduler.IScheduler;
import org.ignis.backend.scheduler.model.IContainerDetails;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IContainerCreateTask extends IContainerTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IContainerCreateTask.class);

    private final List<IContainer> containers;
    private final IScheduler scheduler;

    public IContainerCreateTask(String name, IContainer container, IScheduler scheduler, List<IContainer> containers) {
        super(name, container);
        this.containers = containers;
        this.scheduler = scheduler;
    }
    
    private IContainerDetails parseContainer(){
        IContainerDetails.IContainerDetailsBuilder builder = IContainerDetails.builder();
        //TODO
        return builder.build();
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        List<Integer> stopped = new ArrayList<>();
        if (container.getInfo() != null) {
             for (int i = 0; i < containers.size(); i++) {
                switch (scheduler.getStatus(container.getInfo().getId())) {
                    case DESTROYED:
                    case FINISHED:
                    case ERROR:
                        stopped.add(i);
                        break;
                    default:
                        LOGGER.info(log() + "Container "+i+" already running");
                        if(!containers.get(i).testConnection()){
                            LOGGER.info(log() + "Reconnecting to the container " + i);
                            try{
                            containers.get(i).connect();
                            }catch(IgnisException ex){
                                LOGGER.warn(log() + "Container "+i+" dead");
                                stopped.add(i);
                            }
                        }
                }
            }
        }else{
            for (int i = 0; i < containers.size(); i++) {
                stopped.add(i);
            }
        }
        
        if(stopped.isEmpty()){
            return;
        }
         LOGGER.info(log() + "Starting new containers");   

        String group = container.getProperties().getString(IKeys.GROUP);
        List<String> ids = scheduler.createContainerIntances(group, name, parseContainer(), container.getProperties(), containers.size());
        List<IContainerDetails> details = scheduler.getContainerInstances(ids);
        for (int i = 0; i < containers.size(); i++) {
            containers.get(i).setInfo(details.get(i));
        }
        LOGGER.info(log() + "Connecting to the containers");
        for (int i = 0; i < containers.size(); i++) {
            containers.get(i).connect();
        }

    }
}
