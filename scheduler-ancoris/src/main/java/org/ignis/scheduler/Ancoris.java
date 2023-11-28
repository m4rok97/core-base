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

import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IContainerStatus;

import java.util.List;

/**
 * @author César Pomar
 */
public class Ancoris implements IScheduler {

    public static final String NAME = "ancoris";

    public Ancoris(String url) {
        throw new UnsupportedOperationException("Not supported yet.");
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
    public String createDriverContainer(String group, String name, IContainerInfo container) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<String> createExecutorContainers(String group, String name, IContainerInfo container, int instances)
            throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String createDriverWithExecutorContainers(String group, String driverName,
                                                     IContainerInfo driverContainer,
                                                     List<ExecutorContainers> executorContainers)
            throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IContainerStatus getStatus(String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<IContainerStatus> getStatus(List<String> ids) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IContainerInfo getContainer(String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<IContainerInfo> getExecutorContainers(List<String> ids) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IContainerInfo restartContainer(String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void destroyDriverContainer(String id) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void destroyExecutorInstances(List<String> ids) throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        throw new UnsupportedOperationException("Not supported yet.");
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
