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
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IPropertiesKeys;
import org.ignis.backend.properties.IPropertiesParser;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        LOGGER.info("Backend started");
        IAttributes attributes = new IAttributes();
        LOGGER.info("Checking environment variables");
        String home = System.getenv("IGNIS_HOME");
        if (home == null) {
            LOGGER.error("IGNIS_HOME not exists, aborting");
            return;
        }

        String allocator_url = System.getenv("ANCORIS_URL");
        if (allocator_url != null) {
            LOGGER.error("ANCORIS_URL not exists, aborting");
            return;
        }
        attributes.defaultProperties.setProperty(IPropertiesKeys.ALLOCATOR_URL, allocator_url);

        String dfs = System.getenv("IGNIS_DFS");
        if (dfs == null) {
            LOGGER.error("IGNIS_DFS not exist, aborting");
            return;
        }
        attributes.defaultProperties.setProperty(IPropertiesKeys.DFS_HOME, dfs);

        LOGGER.info("Loading configuration file");
        try {
            attributes.defaultProperties.fromFile("ignis.yaml");
        } catch (Exception ex) {
            LOGGER.error("Error loading ignis.yaml, aborting", ex);
            return;
        }

        TMultiplexedProcessor processor = new TMultiplexedProcessor();

        IBackendServiceImpl backendService = new IBackendServiceImpl(attributes);

        processor.registerProcessor("backend", new IBackendService.Processor<>(backendService));
        processor.registerProcessor("cluster", new IClusterService.Processor<>(new IClusterServiceImpl(attributes)));
        processor.registerProcessor("job", new IJobService.Processor<>(new IJobServiceImpl(attributes)));
        processor.registerProcessor("data", new IDataService.Processor<>(new IDataServiceImpl(attributes)));
        processor.registerProcessor("properties", new IPropertiesService.Processor<>(new IPropertiesServiceImpl(attributes)));

        try {
            Integer port = IPropertiesParser.getInteger(attributes.defaultProperties,
                    IPropertiesKeys.DRIVER_RPC_PORT);
            System.out.println(port);
            backendService.start(processor, port);
        } catch (IgnisException ex) {
            LOGGER.error("Error parsing server port, aborting", ex);
            return;
        }
        LOGGER.info("Backend stopped");
    }

}
