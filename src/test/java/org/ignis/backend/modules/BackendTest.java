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
package org.ignis.backend.modules;

import org.ignis.backend.allocator.IAllocator;
import org.ignis.backend.allocator.IContainerStub;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.services.IAttributes;
import org.ignis.backend.services.IClusterServiceImpl;
import org.ignis.backend.services.IDataServiceImpl;
import org.ignis.backend.services.IJobServiceImpl;
import org.ignis.backend.services.IPropertiesServiceImpl;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class BackendTest {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BackendTest.class);

    protected IAllocator allocator;
    protected IAttributes attributes;
    protected IPropertiesServiceImpl propertiesService;
    protected IClusterServiceImpl clusterService;
    protected IJobServiceImpl jobService;
    protected IDataServiceImpl dataService;

    public BackendTest() {
        /*try {
            allocator = Mockito.spy(ILocalAllocator.class);
            Mockito.doAnswer(a -> {
                IContainerStub stub = Mockito.spy(new ILocalContainerStub(a.getArgument(0)));
                Mockito.doAnswer(a2 -> null).when(stub).request();
                Mockito.doAnswer(a2 -> true).when(stub).isRunning();
                Mockito.doAnswer(a2 -> null).when(stub).destroy();
                return stub;
            }).when(allocator).getContainer(Mockito.any());
            attributes = new IAttributes();
            attributes.defaultProperties.fromFile("src/test/resources/ignis.yaml");
            propertiesService = new IPropertiesServiceImpl(attributes);
            clusterService = new IClusterServiceImpl(attributes, allocator);
            jobService = new IJobServiceImpl(attributes);
            dataService = new IDataServiceImpl(attributes);
        } catch (IgnisException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }*/
    }

}
