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
package org.ignis.backend.properties;

/**
 *
 * @author César Pomar
 */
public final class IKeys {

    /*TOP*/
    public static final String DEBUG = "ignis.debug";
    public static final String HOME = "ignis.home";
    public static final String GROUP = "ignis.group";
    public static final String OPTIONS = "ignis.options";
    /*JOB*/
    public static final String JOB_NAME = "ignis.job.name";
    public static final String JOB_DIRECTORY = "ignis.job.directory";
    /*DFS*/
    public static final String DFS_ID = "ignis.dfs.id";
    public static final String DFS_HOME = "ignis.dfs.home";
    /*SCHEDULER*/
    public static final String SCHEDULER_URL = "ignis.scheduler.url";
    public static final String SCHEDULER_TYPE = "ignis.scheduler.type";
    public static final String SCHEDULER_CONTAINER = "ignis.scheduler.container";
    public static final String SCHEDULER_DNS = "ignis.scheduler.dns";
    /*DRIVER*/
    public static final String DRIVER_IMAGE = "ignis.driver.image";
    public static final String DRIVER_CORES = "ignis.driver.cores";
    public static final String DRIVER_MEMORY = "ignis.driver.memory";
    public static final String DRIVER_RPC_PORT = "ignis.driver.rpc.port";
    public static final String DRIVER_RPC_COMPRESSION = "ignis.driver.rpc.compression";
    public static final String DRIVER_PORT = "ignis.driver.port";
    public static final String DRIVER_PORTS = "ignis.driver.ports";
    public static final String DRIVER_BIND = "ignis.driver.bind";
    public static final String DRIVER_VOLUME = "ignis.driver.volume";
    public static final String DRIVER_HOSTS = "ignis.driver.hosts";
    public static final String DRIVER_ENV = "ignis.driver.env";
    public static final String DRIVER_HEALTHCHECK_PORT = "ignis.driver.healthcheck.port";
    public static final String DRIVER_HEALTHCHECK_INTERVAL = "ignis.driver.healthcheck.interval";
    public static final String DRIVER_HEALTHCHECK_TIMEOUT = "ignis.driver.healthcheck.timeout";
    public static final String DRIVER_HEALTHCHECK_RETRIES = "ignis.driver.healthcheck.retries";
    /*EXECUTOR*/
    public static final String EXECUTOR_IMAGE = "ignis.executor.image";
    public static final String EXECUTOR_CORES = "ignis.executor.cores";
    public static final String EXECUTOR_MEMORY = "ignis.executor.memory";
    public static final String EXECUTOR_RPC_PORT = "ignis.executor.rpc.port";
    public static final String EXECUTOR_RPC_COMPRESSION = "ignis.executor.rpc.compression";
    public static final String EXECUTOR_ISOLATION = "ignis.executor.isolation";
    /*PARTITION*/
    public static final String PARTITION_TYPE = "ignis.partition.type";
    public static final String PARTITION_MINIMAL = "ignis.partition.minimal";
    public static final String PARTITION_COMPRESSION = "ignis.partition.compression";
    public static final String PARTITION_SERIALIZATION = "ignis.partition.serialization";
    public static final String PARTITION_DIRECTORY = "ignis.partition.directory";
    /*TRANSPORT*/
    public static final String TRANSPORT_TYPE = "ignis.transport.type";
    public static final String TRANSPORT_PORT = "ignis.transport.port";
    public static final String TRANSPORT_PORTS = "ignis.transport.ports";
    public static final String TRANSPORT_MINIMAL = "ignis.transport.minimal";
    /*MODULES*/
 /*  IO*/
    public static final String MODULES_IO_OVERWRITE = "ignis.modules.io.overwrite";
    public static final String MODULES_IO_COMPRESSION = "ignis.modules.io.compression";
    /*  SORT*/
    public static final String MODULES_SORT_SAMPLES = "ignis.modules.sort.samples";

    /*DEPRECATED*/
    public static final String DRIVER_SWAP = "ignis.driver.swap";
    public static final String DRIVER_SWAPPINESS = "ignis.driver.swappiness";
    public static final String DRIVER_TASK_MAX_FAILURES = "ignis.driver.task.maxFailures";
    public static final String DRIVER_TASK_MIN_WORKERS = "ignis.driver.task.minWorkers";
    public static final String EXECUTOR_CORES_BUFFER = "ignis.executor.cores.buffer";
    public static final String EXECUTOR_INSTANCES = "ignis.executor.instances";
    public static final String EXECUTOR_LOG = "ignis.executor.log";
    public static final String EXECUTOR_STORAGE_COMPRESSION = "ignis.executor.storage.compression";
    public static final String EXECUTOR_STORAGE = "ignis.executor.storage";
    public static final String EXECUTOR_SWAP = "ignis.executor.swap";
    public static final String EXECUTOR_SWAPPINESS = "ignis.executor.swappiness";
    public static final String MANAGER_EXECUTORS_PORT = "ignis.manager.executors.port";
    public static final String MANAGER_RPC_COMPRESSION = "ignis.manager.rpc.compression";
    public static final String MANAGER_RPC_PORT = "ignis.manager.rpc.port";
    public static final String TRANSPORT_BUFFER = "ignis.transport.buffer";
    public static final String TRANSPORT_COMPRESSION = "ignis.transport.compression";
    public static final String TRANSPORT_SERIALIZATION = "ignis.transport.serialization";
    public static final String TRANSPORT_THREADS = "ignis.transport.threads";

}
