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
import org.ignis.backend.exception.IPropertyException;
import org.ignis.backend.exception.ISchedulerException;
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
import org.ignis.rpc.driver.IDataFrameService;
import org.ignis.rpc.driver.IPropertiesService;
import org.ignis.rpc.driver.IWorkerService;
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

        try {
            LOGGER.info("Getting Driver container info from scheduler");
            attributes.driver.initInfo(scheduler.getContainer(scheduler.getThisContainerId()));
            LOGGER.info("Driver container info found");
        } catch (ISchedulerException ex) {
            LOGGER.error("Not found", ex);
            System.exit(-1);
        }

        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        IBackendServiceImpl backend = null;
        IClusterServiceImpl clusters = null;

        try {
            processor.registerProcessor("backend", new IBackendService.Processor<>(backend = new IBackendServiceImpl(attributes)));
            processor.registerProcessor("cluster", new IClusterService.Processor<>(clusters = new IClusterServiceImpl(attributes, scheduler)));
            processor.registerProcessor("worker", new IWorkerService.Processor<>(new IWorkerServiceImpl(attributes)));
            processor.registerProcessor("dataframe", new IDataFrameService.Processor<>(new IDataFrameServiceImpl(attributes)));
            processor.registerProcessor("properties", new IPropertiesService.Processor<>(new IPropertiesServiceImpl(attributes)));
        } catch (Exception ex) {
            LOGGER.error("Error starting services, aborting", ex);
            System.exit(-1);
        }

        try {
            Integer backendPort = attributes.defaultProperties.getInteger(IKeys.DRIVER_RPC_PORT);
            Integer backendCompression = attributes.defaultProperties.getInteger(IKeys.DRIVER_RPC_COMPRESSION);
            Integer driverPort = attributes.defaultProperties.getInteger(IKeys.EXECUTOR_RPC_PORT);
            Integer driverCompression = attributes.defaultProperties.getInteger(IKeys.EXECUTOR_RPC_COMPRESSION);
            System.out.println(backendPort);
            System.out.println(backendCompression);
            System.out.println(driverPort);
            System.out.println(driverCompression);
            backend.start(processor, backendPort, backendCompression);

            if (!attributes.defaultProperties.contains(IKeys.DEBUG)) {
                clusters.destroyClusters();
            }
            LOGGER.info("Backend stopped");
            System.exit(0);
        } catch (Exception ex) {
            LOGGER.error("Server error, aborting", ex);
            System.exit(-1);
        }
    }

}
