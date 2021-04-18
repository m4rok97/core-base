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

import org.ignis.backend.exception.ISchedulerException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.scheduler.model.IContainerDetails;

import java.util.List;

/**
 * @author César Pomar
 */
public class IAncorisScheduler implements IScheduler {

    public IAncorisScheduler(String url) {

    }

    @Override
    public String createGroup(String name) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void destroyGroup(String group) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String createSingleContainer(String group, String name, IContainerDetails container,
                                        IProperties props) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<String> createContainerIntances(String group, String name, IContainerDetails container,
                                                IProperties props, int instances) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IContainerDetails.ContainerStatus getStatus(String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IContainerDetails getContainer(String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<IContainerDetails> getContainerInstances(List<String> ids) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IContainerDetails restartContainer(String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void destroyContainer(String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void destroyContainerInstaces(List<String> ids) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
