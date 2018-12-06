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

import org.ignis.backend.properties.IPropsKeys;
import org.ignis.backend.rpc.MockClusterServices;
import org.ignis.backend.rpc.MockJobServices;
import org.ignis.rpc.ISource;
import org.ignis.rpc.driver.IDataId;
import org.ignis.rpc.driver.IJobId;
import org.ignis.rpc.executor.IFilesModule;
import org.ignis.rpc.executor.IMapperModule;
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
public class MapperModuleTest extends BackendTest {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MapperModuleTest.class);

    @BeforeAll
    public static void info() {
        LOGGER.info("----|----|----|----MapperModuleTest----|----|----|----");
    }

    public void testMap(int instances) {
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
            mockJob.setMapperModule(Mockito.mock(IMapperModule.Iface.class));
            Mockito.doAnswer(a -> null).when(mockJob.getMapperModule())._map(Mockito.any());
            mockJob.mock();

            IDataId read = jobService.readFile(job, "src/test/resources/LoremIpsum.txt");
            IDataId map = dataService._map(read, new ISource());
            dataService.saveAsTextFile(map, "src/test/salida.txt", true);
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        }
    }

    public void testStreamingMap(int instances) {
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
            mockJob.setMapperModule(Mockito.mock(IMapperModule.Iface.class));
            Mockito.doAnswer(a -> null).when(mockJob.getMapperModule()).streamingMap(Mockito.any(), Mockito.anyBoolean());
            mockJob.mock();

            IDataId read = jobService.readFile(job, "src/test/resources/LoremIpsum.txt");
            IDataId map = dataService.streamingMap(read, new ISource(), true);
            dataService.saveAsTextFile(map, "src/test/salida.txt", true);
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        }
    }

    public void testFlatmap(int instances) {
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
            mockJob.setMapperModule(Mockito.mock(IMapperModule.Iface.class));
            Mockito.doAnswer(a -> null).when(mockJob.getMapperModule()).flatmap(Mockito.any());
            mockJob.mock();

            IDataId read = jobService.readFile(job, "src/test/resources/LoremIpsum.txt");
            IDataId map = dataService.flatmap(read, new ISource());
            dataService.saveAsTextFile(map, "src/test/salida.txt", true);
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        }
    }

    public void testStreamingFlatmap(int instances) {
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
            mockJob.setMapperModule(Mockito.mock(IMapperModule.Iface.class));
            Mockito.doAnswer(a -> null).when(mockJob.getMapperModule()).streamingFlatmap(Mockito.any(), Mockito.anyBoolean());
            mockJob.mock();

            IDataId read = jobService.readFile(job, "src/test/resources/LoremIpsum.txt");
            IDataId map = dataService.streamingMap(read, new ISource(), true);
            dataService.saveAsTextFile(map, "src/test/salida.txt", true);
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        }
    }

    public void testFilter(int instances) {
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
            mockJob.setMapperModule(Mockito.mock(IMapperModule.Iface.class));
            Mockito.doAnswer(a -> null).when(mockJob.getMapperModule()).filter(Mockito.any());
            mockJob.mock();

            IDataId read = jobService.readFile(job, "src/test/resources/LoremIpsum.txt");
            IDataId map = dataService.filter(read, new ISource());
            dataService.saveAsTextFile(map, "src/test/salida.txt", true);
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        }
    }

    public void testStreamingFilter(int instances) {
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
            mockJob.setMapperModule(Mockito.mock(IMapperModule.Iface.class));
            Mockito.doAnswer(a -> null).when(mockJob.getMapperModule()).streamingFilter(Mockito.any(), Mockito.anyBoolean());
            mockJob.mock();

            IDataId read = jobService.readFile(job, "src/test/resources/LoremIpsum.txt");
            IDataId map = dataService.streamingFilter(read, new ISource(), true);
            dataService.saveAsTextFile(map, "src/test/salida.txt", true);
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        }
    }
    
        public void testKeyBy(int instances) {
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
            mockJob.setMapperModule(Mockito.mock(IMapperModule.Iface.class));
            Mockito.doAnswer(a -> null).when(mockJob.getMapperModule())._map(Mockito.any());
            mockJob.mock();

            IDataId read = jobService.readFile(job, "src/test/resources/LoremIpsum.txt");
            IDataId map = dataService.keyBy(read, new ISource());
            dataService.saveAsTextFile(map, "src/test/salida.txt", true);
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        }
    }

    @Test
    public void testMapOneInstance() {
        LOGGER.info("----|----testMapOneInstance----|----");
        testMap(1);
    }

    @Test
    public void testMapMultipleInstance() {
        LOGGER.info("----|----testMapMultipleInstance----|----");
        testMap(10);
    }

    @Test
    public void testStreamingMapOneInstance() {
        LOGGER.info("----|----testStreamingMapOneInstance----|----");
        testStreamingMap(1);
    }

    @Test
    public void testStreamingMapMultipleInstance() {
        LOGGER.info("----|----testStreamingMapMultipleInstance----|----");
        testStreamingMap(10);
    }

    @Test
    public void testFlatmapOneInstance() {
        LOGGER.info("----|----testFlatmapOneInstance----|----");
        testFlatmap(1);
    }

    @Test
    public void testFlatmapMultipleInstance() {
        LOGGER.info("----|----testFlatmapMultipleInstance----|----");
        testFlatmap(10);
    }

    @Test
    public void testStreamingFlatmapOneInstance() {
        LOGGER.info("----|----testStreamingFlatmapOneInstance----|----");
        testStreamingFlatmap(1);
    }

    @Test
    public void testStreamingFlatmapMultipleInstance() {
        LOGGER.info("----|----testStreamingFlatmapMultipleInstance----|----");
        testStreamingFlatmap(10);
    }

    @Test
    public void testFilterOneInstance() {
        LOGGER.info("----|----testFilterOneInstance----|----");
        testFilter(1);
    }

    @Test
    public void testFilterMultipleInstance() {
        LOGGER.info("----|----testFilterMultipleInstance----|----");
        testFilter(10);
    }

    @Test
    public void testStreamingFilterOneInstance() {
        LOGGER.info("----|----testStreamingFilterOneInstance----|----");
        testStreamingFilter(1);
    }

    @Test
    public void testStreamingFilterMultipleInstance() {
        LOGGER.info("----|----testStreamingFilterMultipleInstance----|----");
        testStreamingFilter(10);
    }
}
