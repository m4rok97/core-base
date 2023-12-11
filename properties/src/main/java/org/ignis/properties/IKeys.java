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
package org.ignis.properties;

/**
 * @author César Pomar
 */
public final class IKeys {

    /*TOP*/
    public static final String DEBUG = "ignis.debug";
    public static final String HOME = "ignis.home";
    public static final String OPTIONS = "ignis.options";
    public static final String REGISTRY = "ignis.registry";
    public static final String WORKING_DIRECTORY = "ignis.wdir";
    public static final String TMPDIR = "ignis.tmpdir";
    public static final String PORT = "ignis.port";

    public static final String TIME = "ignis.time";
    /*JOB*/
    public static final String JOB_ID = "ignis.job.id";
    public static final String JOB_NAME = "ignis.job.name";

    public static final String JOB_TIME = "ignis.job.time";
    public static final String JOB_DIRECTORY = "ignis.job.directory";
    public static final String JOB_WORKER = "ignis.job.worker";
    /*DFS*/
    public static final String DFS_ID = "ignis.dfs.id";
    public static final String DFS_HOME = "ignis.dfs.home";
    /*SCHEDULER*/
    public static final String SCHEDULER_URL = "ignis.scheduler.url";
    public static final String SCHEDULER_TYPE = "ignis.scheduler.type";
    public static final String SCHEDULER_DNS = "ignis.scheduler.dns";
    public static final String SCHEDULER_PARAMS = "ignis.scheduler.param";
    /*DRIVER*/
    public static final String DRIVER = "ignis.driver";
    public static final String DRIVER_NAME = "ignis.driver.name";
    public static final String DRIVER_IMAGE = "ignis.driver.image";
    public static final String DRIVER_CORES = "ignis.driver.cores";
    public static final String DRIVER_GPU = "ignis.driver.gpu";
    public static final String DRIVER_MEMORY = "ignis.driver.memory";
    public static final String DRIVER_RPC_PORT = "ignis.driver.rpc.port";
    public static final String DRIVER_RPC_COMPRESSION = "ignis.driver.rpc.compression";
    public static final String DRIVER_RPC_POOL = "ignis.driver.pool";
    public static final String DRIVER_HOST = "ignis.driver.node";
    public static final String DRIVER_NODELIST = "ignis.driver.nodelist";
    public static final String DRIVER_PORTS = "ignis.driver.ports";
    public static final String DRIVER_BINDS = "ignis.driver.binds";
    public static final String DRIVER_VOLUME = "ignis.driver.volume";
    public static final String DRIVER_HOSTNAMES = "ignis.driver.hostnames";
    public static final String DRIVER_ENV = "ignis.driver.env";
    public static final String DRIVER_PUBLIC_KEY = "ignis.driver.public.key";//TOREMOVE
    public static final String DRIVER_PRIVATE_KEY = "ignis.driver.private.key";//TOREMOVE
    public static final String CRYPTO_SECRET = "ignis.crypto.secret";
    public static final String CRYPTO_PUBLIC = "ignis.crypto.public";
    public static final String CRYPTO_PRIVATE = "ignis.crypto.private";
    public static final String DRIVER_HEALTHCHECK_PORT = "ignis.driver.healthcheck.port";
    public static final String DRIVER_HEALTHCHECK_URL = "ignis.driver.healthcheck.url";
    public static final String DRIVER_HEALTHCHECK_INTERVAL = "ignis.driver.healthcheck.interval";
    public static final String DRIVER_HEALTHCHECK_TIMEOUT = "ignis.driver.healthcheck.timeout";
    public static final String DRIVER_HEALTHCHECK_RETRIES = "ignis.driver.healthcheck.retries";

    public static final String DISCOVERY_TYPE = "ignis.discovery.type";
    public static final String DISCOVERY_TARGER = "ignis.discovery.target";

    /*EXECUTOR*/

    public static final String EXECUTOR = "ignis.executor";
    public static final String EXECUTOR_NAME = "ignis.executor.name";
    public static final String EXECUTOR_INSTANCES = "ignis.executor.instances";
    public static final String EXECUTOR_ATTEMPTS = "ignis.executor.attempts";
    public static final String EXECUTOR_IMAGE = "ignis.executor.image";
    public static final String EXECUTOR_CORES = "ignis.executor.cores";
    public static final String EXECUTOR_GPU = "ignis.executor.gpu";
    public static final String EXECUTOR_CORES_SINGLE = "ignis.executor.cores.single";
    public static final String EXECUTOR_MEMORY = "ignis.executor.memory";
    public static final String EXECUTOR_RPC_PORT = "ignis.executor.rpc.port";
    public static final String EXECUTOR_RPC_COMPRESSION = "ignis.executor.rpc.compression";
    public static final String EXECUTOR_ISOLATION = "ignis.executor.isolation";
    public static final String EXECUTOR_DIRECTORY = "ignis.executor.directory";
    public static final String EXECUTOR_HOST = "ignis.executor.node";
    public static final String EXECUTOR_NODELIST = "ignis.executor.nodelist";
    public static final String EXECUTOR_PORTS = "ignis.executor.ports";
    public static final String EXECUTOR_BINDS = "ignis.executor.binds";
    public static final String EXECUTOR_VOLUME = "ignis.executor.volume";
    public static final String EXECUTOR_HOSTNAMES = "ignis.executor.hostnames";
    public static final String EXECUTOR_ENV = "ignis.executor.env";
    /*PARTITION*/
    public static final String PARTITION_TYPE = "ignis.partition.type";
    public static final String PARTITION_MINIMAL = "ignis.partition.minimal";
    public static final String PARTITION_COMPRESSION = "ignis.partition.compression";
    public static final String PARTITION_SERIALIZATION = "ignis.partition.serialization";
    /*TRANSPORT*/
    public static final String TRANSPORT_CORES = "ignis.transport.cores";
    public static final String TRANSPORT_COMPRESSION = "ignis.transport.compression";
    public static final String TRANSPORT_PORTS = "ignis.transport.ports";
    public static final String TRANSPORT_ELEMENT_SIZE = "ignis.transport.element.size";
    public static final String TRANSPORT_MINIMAL = "ignis.transport.minimal";
    /*MODULES*/
    /*IO*/
    public static final String MODULES_IO_OVERWRITE = "ignis.modules.io.overwrite";
    /*  SORT*/
    public static final String MODULES_SORT_SAMPLES = "ignis.modules.sort.samples";

    public static final String CONTAINER_DOCKER_REGISTRY = "ignis.container.docker.registry";
    public static final String CONTAINER_DOCKER_NAMESPACE = "ignis.container.docker.namespace";
    public static final String CONTAINER_DOCKER_DEFAULT = "ignis.container.docker.default";
    public static final String CONTAINER_DOCKER_TAG = "ignis.container.docker.tag";
    public static final String CONTAINER_DOCKER_ROOT = "ignis.container.docker.root";
    public static final String CONTAINER_DOCKER_NETWORK = "ignis.container.docker.network";
    public static final String CONTAINER_SINGULARITY_SOURCE = "ignis.container.singularity.source";
    public static final String CONTAINER_SINGULARITY_DEFAULT = "ignis.container.singularity.default";
    public static final String CONTAINER_SINGULARITY_NETWORK = "ignis.container.singularity.network";
    public static final String CONTAINER_WRITABLE = "ignis.container.writable";
    public static final String CONTAINER_PROVIDER = "ignis.container.provider";


}
