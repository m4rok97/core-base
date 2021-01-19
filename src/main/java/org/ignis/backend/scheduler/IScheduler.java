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

import java.util.List;
import org.ignis.backend.exception.ISchedulerException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.scheduler.model.IContainerDetails;

/**
 *
 * @author César Pomar
 */
public interface IScheduler {

    /**
     *
     * @param name
     * @return
     * @throws ISchedulerException
     */
    public String createGroup(String name) throws ISchedulerException;

    /**
     *
     * @param group
     * @throws ISchedulerException
     */
    public void destroyGroup(String group) throws ISchedulerException;

    /**
     *
     * @param group
     * @param name
     * @param container
     * @param props
     * @return
     * @throws ISchedulerException
     */
    public String createSingleContainer(String group, String name, IContainerDetails container, IProperties props)
            throws ISchedulerException;

    /**
     *
     * @param group
     * @param name
     * @param container
     * @param props
     * @param instances
     * @return
     * @throws ISchedulerException
     */
    public List<String> createContainerIntances(String group, String name, IContainerDetails container,
            IProperties props, int instances) throws ISchedulerException;

    /**
     *
     * @param id
     * @return
     * @throws ISchedulerException
     */
    public IContainerDetails.ContainerStatus getStatus(String id) throws ISchedulerException;

    /**
     *
     * @param id
     * @return
     * @throws ISchedulerException
     */
    public IContainerDetails getContainer(String id) throws ISchedulerException;

    /**
     *
     * @param ids
     * @return
     * @throws ISchedulerException
     */
    public List<IContainerDetails> getContainerInstances(List<String> ids) throws ISchedulerException;

    /**
     *
     * @param id
     * @throws ISchedulerException
     */
    public IContainerDetails restartContainer(String id) throws ISchedulerException;

    /**
     *
     * @param id
     * @throws ISchedulerException
     */
    public void destroyContainer(String id) throws ISchedulerException;
    
        /**
     *
     * @param ids
     * @throws ISchedulerException
     */
    public void destroyContainerInstaces(List<String> ids) throws ISchedulerException;

    /**
     *
     * @throws ISchedulerException
     */
    public void healthCheck() throws ISchedulerException;

}
