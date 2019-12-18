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

import java.io.File;
import java.io.IOException;
import org.apache.thrift.TMultiplexedProcessor;
import org.ignis.backend.allocator.IAllocator;
import org.ignis.backend.exception.IPropertyException;
import org.ignis.backend.exception.ISchedulerException;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.scheduler.IScheduler;
import org.ignis.backend.scheduler.ISchedulerBuilder;
import org.ignis.backend.services.IAttributes;
import org.ignis.backend.services.IBackendServiceImpl;
import org.ignis.backend.services.IClusterServiceImpl;
import org.ignis.backend.services.IDataFrameServiceImpl;
import org.ignis.backend.services.IWorkerServiceImpl;
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

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        LOGGER.info("Backend started");
        IAttributes attributes = new IAttributes();

        LOGGER.info("Loading environment variables");
        attributes.defaultProperties.fromEnv(System.getenv());//Only for IGNIS_HOME

        LOGGER.info("Loading configuration file");
        try {
            String conf = new File(attributes.defaultProperties.getString(IKeys.HOME), "etc/ignis.conf").getPath();
            attributes.defaultProperties.load(conf);
        } catch (IPropertyException | IOException ex) {
            LOGGER.error("Error loading ignis.conf, aborting", ex);
            System.exit(-1);
        }

        LOGGER.info("Loading scheduler");
        IScheduler scheduler = null;
        String schedulerType = null;
        String schedulerUrl = null;
        try {
            schedulerType = attributes.defaultProperties.getString(IKeys.SCHEDULER_TYPE);
            schedulerUrl = attributes.defaultProperties.getString(IKeys.SCHEDULER_URL);
        } catch (IPropertyException ex) {
            LOGGER.error(ex.getMessage(), ex);
            System.exit(-1);
        }
        try {
            LOGGER.info("Checking scheduler " + schedulerType);
            scheduler = ISchedulerBuilder.create(schedulerType, schedulerUrl);
            scheduler.healthCheck();
            LOGGER.info("Scheduler " + schedulerType + " " + schedulerUrl + " ... OK");
        } catch (ISchedulerException ex) {
            LOGGER.error("Scheduler " + schedulerType + " " + schedulerUrl + " ... Fails\n" + ex);
            System.exit(-1);
        }

        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        IBackendServiceImpl backend = null;
        
        try {
            processor.registerProcessor("backend", new IBackendService.Processor<>(backend = new IBackendServiceImpl(attributes)));
            processor.registerProcessor("cluster", new IClusterService.Processor<>(new IClusterServiceImpl(attributes, scheduler)));
            processor.registerProcessor("worker", new IJobService.Processor<>(new IWorkerServiceImpl(attributes)));
            processor.registerProcessor("dataframe", new IDataService.Processor<>(new IDataFrameServiceImpl(attributes)));
            processor.registerProcessor("properties", new IPropertiesService.Processor<>(new IPropertiesServiceImpl(attributes)));
        } catch (Exception ex) {
            LOGGER.error("Error starting services, aborting", ex);
            System.exit(-1);
        }

        try {
            Integer port = attributes.defaultProperties.getInteger(IKeys.DRIVER_RPC_PORT);
            Integer compression = attributes.defaultProperties.getInteger(IKeys.DRIVER_RPC_COMPRESSION);
            System.out.println(port);
            System.out.println(compression);
            backend.start(processor, port);

            if (!attributes.defaultProperties.contains(IKeys.DEBUG)) {
                attributes.destroyClusters();
            }
            LOGGER.info("Backend stopped");

        } catch (Exception ex) {
            LOGGER.error("Server error, aborting", ex);
            System.exit(-1);
        }
    }

}
