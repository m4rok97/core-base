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

import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IPropsKeys;
import org.ignis.backend.rpc.MockClusterServices;
import org.ignis.backend.rpc.MockJobServices;
import org.ignis.rpc.driver.IDataId;
import org.ignis.rpc.driver.IJobId;
import org.ignis.rpc.executor.IFilesModule;
import org.ignis.rpc.manager.IRegisterManager;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class FilesModuleTest extends BackendTest {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FilesModuleTest.class);

    @BeforeAll
    public static void info() {
        LOGGER.info("----|----|----|----FilesModuleTest----|----|----|----");
    }

    public void testReadAndWrite(int instances) {
        try {
            long prop = propertiesService.newInstance();
            attributes.getProperties(prop).setProperty(IPropsKeys.EXECUTOR_INSTANCES, String.valueOf(instances));

            long cluster = clusterService.newInstance(prop);
            MockClusterServices mockCluster = new MockClusterServices(attributes.getCluster(cluster));
            mockCluster.setRegisterManager(Mockito.mock(IRegisterManager.Iface.class));
            Mockito.doAnswer(a -> null).when(mockCluster.getRegisterManager()).execute(Mockito.anyInt(), Mockito.any());
            mockCluster.mock();

            IJobId job = jobService.newInstance(cluster, "none");
            MockJobServices mockJob = new MockJobServices(attributes.getCluster(cluster).getJob(job.getJob()));
            mockJob.setFilesModule(Mockito.mock(IFilesModule.Iface.class));
            Mockito.doAnswer(a -> null).when(mockJob.getFilesModule()).readFile(Mockito.any(), Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
            Mockito.doAnswer(a -> null).when(mockJob.getFilesModule()).saveFile(Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean());
            mockJob.mock();

            IDataId read = jobService.readFile(job, "src/test/resources/LoremIpsum.txt");
            dataService.saveAsTextFile(read, "src/test/salida.txt", true);
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        }
    }

    
    public void testOneInstance() {
        LOGGER.info("----|----testOneInstance----|----");
        testReadAndWrite(1);
    }

    @Test
    public void testMultipleInstance() {
        try{
            Integer.parseInt("no");
        }catch(Exception ex){
            System.out.println(new IgnisException("holita",ex).toString());
        }
    }

}
