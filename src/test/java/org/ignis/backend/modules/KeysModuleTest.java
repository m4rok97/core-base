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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.ignis.backend.properties.IPropsKeys;
import org.ignis.backend.rpc.MockClusterServices;
import org.ignis.backend.rpc.MockJobServices;
import org.ignis.rpc.ISource;
import org.ignis.rpc.driver.IDataId;
import org.ignis.rpc.driver.IJobId;
import org.ignis.rpc.executor.IFilesModule;
import org.ignis.rpc.executor.IKeysModule;
import org.ignis.rpc.executor.IPostmanModule;
import org.ignis.rpc.executor.IReducerModule;
import org.ignis.rpc.executor.IShuffleModule;
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
public class KeysModuleTest extends BackendTest {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(KeysModuleTest.class);

    @BeforeAll
    public static void info() {
        LOGGER.info("----|----|----|----KeysModuleTest----|----|----|----");
    }

    @SuppressWarnings("unchecked")
    public void testReduceByKey(int instances) {
        Random random = new Random(0);
        List<List<Long>> keys = new ArrayList<>();
        for (int i = 0; i < instances; i++) {
            keys.add(new ArrayList<>());
            for (int j = 0; j < 100 + i; j++) {
                keys.get(i).add((Long) (long) random.nextInt(20 * instances));
            }
        }

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
            mockJob.setShuffleModule(Mockito.mock(IShuffleModule.Iface.class, a -> null));
            mockJob.setPostmanModule(Mockito.mock(IPostmanModule.Iface.class, a -> null));
            mockJob.setReducerModule(Mockito.mock(IReducerModule.Iface.class, a -> null));
            mockJob.setKeysModule(Mockito.mock(IKeysModule.Iface.class, a -> null));
            Mockito.when(mockJob.getKeysModule().getKeys()).thenReturn(
                    keys.get(0), 
                    keys.size() > 0 ? keys.subList(1, keys.size()).toArray(new List[0]) : new List[0]
            );
            mockJob.mock();

            IDataId read = jobService.readFile(job, "src/test/resources/LoremIpsum.txt");
            IDataId map = dataService.reduceByKey(read, new ISource());
            dataService.saveAsTextFile(map, "src/test/salida.txt", true);
        } catch (Exception ex) {
            Assert.fail(ex.toString());
        }
    }

    @Test
    public void testOneInstance() {
        LOGGER.info("----|----testOneInstance----|----");
        testReduceByKey(1);
    }

    @Test
    public void testMultipleInstance() {
        LOGGER.info("----|----testMultipleInstance----|----");
        testReduceByKey(10);
    }

}
