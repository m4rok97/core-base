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
import org.ignis.backend.cluster.IDriver;
import org.ignis.backend.cluster.tasks.IThreadPool;
import org.ignis.backend.services.*;
import org.ignis.logging.ILogger;
import org.ignis.properties.ICrypto;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.properties.IPropertyException;
import org.ignis.rpc.driver.IBackendService;
import org.ignis.rpc.driver.IClusterService;
import org.ignis.rpc.driver.IPropertiesService;
import org.ignis.rpc.driver.IDataFrameService;
import org.ignis.rpc.driver.IWorkerService;
import org.ignis.scheduler3.IScheduler;
import org.ignis.scheduler3.ISchedulerException;
import org.ignis.scheduler3.ISchedulerFactory;
import org.ignis.scheduler3.model.IClusterInfo;
import org.ignis.scheduler3.model.IContainerInfo;
import org.ignis.scheduler3.model.IJobInfo;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;

/**
 * @author César Pomar
 */
public final class Main {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Main.class);

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        ILogger.init(Boolean.getBoolean(IProperties.asEnv(IKeys.DEBUG)),
                Boolean.getBoolean(IProperties.asEnv(IKeys.VERBOSE)));
        LOGGER.info("Backend started");
        var props = new IProperties();
        try {
            props.load(Main.class.getClassLoader().getResourceAsStream("etc/ignis.yaml"));
        } catch (IOException ex) {
            LOGGER.warn("Default options load error", ex);
        }

        LOGGER.info("Loading configuration file");
        try {
            props.load(new File(props.getString(IKeys.HOME), "etc/ignis.yaml").getPath());
        } catch (IPropertyException | IOException ex) {
            LOGGER.error("Error loading ignis.yaml", ex);
        }

        LOGGER.info("Loading environment variables");
        props.fromEnv(System.getenv());
        var home = props.getString(IKeys.HOME);

        List<IProperties> staticProps = new ArrayList<>();
        if (props.hasProperty(IKeys.OPTIONS)) {//Submit user options
            try {
                var array = props.getProperty(IKeys.OPTIONS).split(";");
                props.load64(array[0]);
                for (int i = 1; i < array.length; i++) {
                    var tmp = new IProperties(props);
                    tmp.load64(array[i]);
                    staticProps.add(tmp);
                }
            } catch (IOException ex) {
                LOGGER.warn("User options load error", ex);
            }
            props.rmProperty(IKeys.OPTIONS);
        }
        //Submitter home may be different so we ignore it.
        props.setProperty(IKeys.HOME, home);

        if (props.getBoolean(IKeys.DEBUG)) {
            System.setProperty(IKeys.DEBUG, "true");
            LOGGER.info("DEBUG enabled");
        } else {
            System.setProperty(IKeys.DEBUG, "false");
        }

        LOGGER.info("Loading scheduler");
        IScheduler scheduler = null;
        {
            String url = "", type = null;
            try {
                url = props.getProperty(IKeys.SCHEDULER_URL, null);
                type = props.getProperty(IKeys.SCHEDULER_NAME);

                LOGGER.info("Checking scheduler " + type);
                scheduler = ISchedulerFactory.create(type, url);
                scheduler.healthCheck();
                LOGGER.info("Scheduler " + type + (url != null ? (" '" + url + "'") : "") + "...OK");
            } catch (ISchedulerException ex) {
                LOGGER.error("Scheduler " + type + (url != null ? (" '" + url + "'") : "") + "...Fails", ex);
                System.exit(-1);
            }
        }

        if (props.hasProperty(IKeys.CRYPTO_SECRET)) {
            var path = new File(props.getProperty(IKeys.CRYPTO_SECRET));
            if (!path.isFile()) {
                throw new IPropertyException(IKeys.CRYPTO_SECRET, path + " not found error");
            }
            LOGGER.info(IKeys.CRYPTO_SECRET + " set");
        } else {
            LOGGER.info(IKeys.CRYPTO_SECRET + " not set");
            for (var entry : props.toMap(true).keySet()) {
                if (IProperties.isCrypted(IProperties.basekey(entry))) {
                    LOGGER.error(IKeys.CRYPTO_SECRET + " is required to avoid security problems or decode encrypted properties");
                    break;
                }
            }
        }

        IJobInfo job = null;
        try {
            LOGGER.info("Getting Job from scheduler");
            job = scheduler.getJob(props.getProperty(IKeys.JOB_ID));
            LOGGER.info("Job found");
        } catch (ISchedulerException ex) {
            LOGGER.error("Job not found", ex);
            System.exit(-1);
        }

        IContainerInfo driver = null;
        for (var cluster : job.clusters()) {
            if (cluster.id().startsWith("0")) {
                driver = cluster.containers().get(0);
                break;
            }
        }

        if (driver == null) {
            LOGGER.error("Driver not found");
            System.exit(-1);
        }

        if (driver.network().equals(IContainerInfo.INetworkMode.HOST)) {
            LOGGER.info("Backend is running in network host mode, properties 'ignis.ports.*' will be ignored");
        }

        LOGGER.info("Creating job folders");
        var perm = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));

        try {
            Files.createDirectory(Path.of(props.getProperty(IKeys.JOB_CONTAINER_DIR)), perm);
        } catch (FileAlreadyExistsException ex) {
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
        try {
            Files.createDirectory(Path.of(props.getProperty(IKeys.JOB_SOCKETS)), perm);
        } catch (FileAlreadyExistsException ex) {
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }

        if (!props.hasProperty(IKeys.CRYPTO_$PRIVATE$)) {
            var pair = ICrypto.genKeyPair();
            props.setProperty(IKeys.CRYPTO_PUBLIC, pair.publicKey());
            props.setProperty(IKeys.CRYPTO_$PRIVATE$, pair.privateKey());
        }

        IThreadPool pool = new IThreadPool(props);
        IServiceStorage ss = new IServiceStorage(new IDriver(props, driver), pool);

        if (Boolean.getBoolean(IKeys.DEBUG)) {
            LOGGER.info("Debug: " + props);
        }

        TMultiplexedProcessor processor = new TMultiplexedProcessor();

        IBackendServiceImpl backend = null;
        IClusterServiceImpl clusters = null;

        try {
            processor.registerProcessor("IBackend", new IBackendService.Processor<>(backend = new IBackendServiceImpl(ss)));
            processor.registerProcessor("ICluster", new IClusterService.Processor<>(clusters = new IClusterServiceImpl(ss, scheduler)));
            processor.registerProcessor("IWorker", new IWorkerService.Processor<>(new IWorkerServiceImpl(ss)));
            processor.registerProcessor("IDataFrame", new IDataFrameService.Processor<>(new IDataFrameServiceImpl(ss)));
            processor.registerProcessor("IProperties", new IPropertiesService.Processor<>(new IPropertiesServiceImpl(ss, staticProps)));
        } catch (Exception ex) {
            LOGGER.error("Error creating services, aborting", ex);
            System.exit(-1);
        }

        try {
            backend.start(processor);

            if (!Boolean.getBoolean(IKeys.DEBUG)) {
                clusters.destroyClusters();
            }

            try {
                if (ss.driver.getExecutor().isConnected()) {
                    ss.driver.getExecutor().getExecutorServerModule().stop();
                    ss.driver.getExecutor().disconnect();
                }
            } catch (Exception ex) {
                LOGGER.warn("Driver callback could not be stopped");
            }

            LOGGER.info("Backend stopped");
            System.exit(0);
        } catch (Exception ex) {
            LOGGER.error("Server error, aborting", ex);
            System.exit(-1);
        }
    }

}
