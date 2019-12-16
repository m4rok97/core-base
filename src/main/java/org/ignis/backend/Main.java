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
package org.ignis.backend;

import org.apache.thrift.TMultiplexedProcessor;
import org.ignis.backend.allocator.IAllocator;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.services.IAttributes;
import org.ignis.backend.services.IBackendServiceImpl;
import org.ignis.backend.services.IClusterServiceImpl;
import org.ignis.backend.services.IDataServiceImpl;
import org.ignis.backend.services.IJobServiceImpl;
import org.ignis.backend.services.IPropertiesServiceImpl;
import org.ignis.rpc.driver.IBackendService;
import org.ignis.rpc.driver.IClusterService;
import org.ignis.rpc.driver.IDataService;
import org.ignis.rpc.driver.IJobService;
import org.ignis.rpc.driver.IPropertiesService;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class Main {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static String loadVarEnv(IProperties properties, String key, String var) {
        String value = System.getenv(var);
        if (value == null) {
            LOGGER.error(var + " not exists, aborting");
            System.exit(-1);
        }
        properties.setProperty(key, value);
        return value;
    }

    public static IAllocator loadAllocator(IProperties properties) {
        String type = properties.getProperty(IKeys.SCHEDULER_TYPE);
        switch (type) {
            case "ancoris":
                loadVarEnv(properties, IKeys.SCHEDULER_URL, "ALLOCATOR_URL");
                //return new IAncorisAllocator(properties.getProperty(IKeys.ALLOCATOR_URL));
            case "ignis":
                throw new UnsupportedOperationException("Allocator not implemented yet.");
            case "local":
                //return new ILocalAllocator();
            default:
                LOGGER.error(type + " is not a valid allocator, aborting");
                System.exit(-1);
        }
        return null;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        LOGGER.info("Backend started");
        IAttributes attributes = new IAttributes();

        LOGGER.info("Loading environment variables");
        String home = loadVarEnv(attributes.defaultProperties, IKeys.HOME, "IGNIS_HOME");
        loadVarEnv(attributes.defaultProperties, IKeys.DFS_HOME, "DFS_HOME");
        loadVarEnv(attributes.defaultProperties, IKeys.DFS_ID, "DFS_ID");

        LOGGER.info("Loading configuration file");
        /*try {
            attributes.defaultProperties.fromFile(new File(home, "etc/ignis.yaml").getPath());
        } catch (IgnisException ex) {
            LOGGER.error("Error loading ignis.yaml, aborting", ex);
            return;
        }*/

        LOGGER.info("Loading allocator");
        IAllocator allocator = loadAllocator(attributes.defaultProperties);
        try {
            LOGGER.info("Checking allocator");
            allocator.ping();
            LOGGER.info("Allocator " + allocator.getName() + " ... OK");
        } catch (IgnisException ex) {
            LOGGER.error("Allocator " + allocator.getName() + " ... Fails\n" + ex);
            System.exit(-1);
        }

        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        IBackendServiceImpl backend;
        try {
            processor.registerProcessor("backend", new IBackendService.Processor<>(backend = new IBackendServiceImpl(attributes)));
            processor.registerProcessor("cluster", new IClusterService.Processor<>(new IClusterServiceImpl(attributes, allocator)));
            processor.registerProcessor("job", new IJobService.Processor<>(new IJobServiceImpl(attributes)));
            processor.registerProcessor("data", new IDataService.Processor<>(new IDataServiceImpl(attributes)));
            processor.registerProcessor("properties", new IPropertiesService.Processor<>(new IPropertiesServiceImpl(attributes)));
        } catch (IgnisException ex) {
            LOGGER.error("Error starting services, aborting", ex);
            return;
        }
/*
        try {
            Integer port = attributes.defaultProperties.getInteger(IKeys.DRIVER_RPC_PORT);
            System.out.println(port);
            backend.start(processor, port);
        } catch (IgnisException ex) {
            LOGGER.error("Error parsing server port, aborting", ex);
            return;
        }
        if(!attributes.defaultProperties.isProperty(IKeys.DEBUG)){
            attributes.destroyClusters();
        }*/
        LOGGER.info("Backend stopped");
    }

}
