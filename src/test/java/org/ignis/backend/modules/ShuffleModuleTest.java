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

import java.util.Arrays;
import org.ignis.backend.properties.IPropsKeys;
import org.ignis.backend.rpc.MockClusterServices;
import org.ignis.backend.rpc.MockJobServices;
import org.ignis.rpc.driver.IDataId;
import org.ignis.rpc.driver.IJobId;
import org.ignis.rpc.executor.IFilesModule;
import org.ignis.rpc.executor.IPostmanModule;
import org.ignis.rpc.executor.IShuffleModule;
import org.ignis.rpc.executor.IStorageModule;
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
public class ShuffleModuleTest extends BackendTest {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ShuffleModuleTest.class);
    
    @BeforeAll
    public static void info() {
        LOGGER.info("----|----|----|----ShuffleModuleTest----|----|----|----");
    }

    public void testShuffle(int instances, Long[] count) {
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
            Mockito.doAnswer(a -> null).when(mockJob.getFilesModule()).saveFile(Mockito.any(), Mockito.anyBoolean());
            mockJob.setShuffleModule(Mockito.mock(IShuffleModule.Iface.class, a -> null));
            mockJob.setPostmanModule(Mockito.mock(IPostmanModule.Iface.class, a -> null));
            mockJob.setStorageModule(Mockito.mock(IStorageModule.Iface.class));
            Mockito.when(mockJob.getStorageModule().count()).thenReturn(count[0], Arrays.copyOfRange(count, 1, count.length));
            mockJob.mock();

            IDataId read = jobService.readFile(job, "src/test/resources/LoremIpsum.txt");
            IDataId shuffle = dataService.shuffle(read);
            dataService.saveAsTextFile(shuffle, "src/test/salida.txt", true);
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        }
    }

    public void testImport(int from, int to) {

    }

    @Test
    public void testShuffleOneInstance() {
        LOGGER.info("----|----testShuffleOneInstance----|----");
        testShuffle(1, new Long[]{100l});
    }

    @Test
    public void testShuffleMultipleInstance() {
        LOGGER.info("----|----testShuffleMultipleInstance----|----");
        testShuffle(10, new Long[]{10l, 100l, 20l, 25l, 0l, 30l, 28l, 20l, 20l, 50l});
    }

    //@Test
    public void testImportOnetoOne() {
        LOGGER.info("----|----testImportOnetoOne----|----");
        testImport(1, 1);
    }

    //@Test
    public void testImportOnetoMultiple() {
        LOGGER.info("----|----testImportOnetoMultiple----|----");
        testImport(1, 10);
    }

    //@Test
    public void testImportMultipletoOne() {
        LOGGER.info("----|----testImportMultipletoOne----|----");
        testImport(10, 1);
    }

    //@Test
    public void testImportMultipletoSameMultiple() {
        LOGGER.info("----|----testImportMultipletoSameMultiple----|----");
        testImport(10, 10);
    }

    public void testImportMultipletoDiferentMultiple() {
        LOGGER.info("----|----testImportMultipletoDiferentMultiple----|----");
        testImport(7, 5);
    }

}
