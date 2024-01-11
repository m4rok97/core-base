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

    public static final String DEBUG = "ignis.debug";
    public static final String VERBOSE = "ignis.verbose";
    public static final String HOME = "ignis.home";
    public static final String OPTIONS = "ignis.options";
    public static final String WDIR = "ignis.wdir";
    public static final String TMPDIR = "ignis.tmpdir";
    public static final String PORT = "ignis.port";
    public static final String TIME = "ignis.time";

    public static final String DRIVER = "ignis.driver";
    public static final String DRIVER_NAME = "ignis.driver.name";
    public static final String DRIVER_IMAGE = "ignis.driver.image";
    public static final String DRIVER_CORES = "ignis.driver.cores";
    public static final String DRIVER_MEMORY = "ignis.driver.memory";
    public static final String DRIVER_GPU = "ignis.driver.gpu";
    public static final String DRIVER_NODE = "ignis.driver.node";
    public static final String DRIVER_NODELIST = "ignis.driver.nodelist";
    public static final String DRIVER_PORTS = "ignis.driver.ports";
    public static final String DRIVER_BINDS = "ignis.driver.binds";
    public static final String DRIVER_ENV = "ignis.driver.env";

    public static final String DISCOVERY_TYPE = "ignis.discovery.type";
    public static final String DISCOVERY_ETCD_CA = "ignis.discovery.etcd.ca";
    public static final String DISCOVERY_ETCD_ENDPOINT = "ignis.discovery.etcd.endpoint";
    public static final String DISCOVERY_ETCD_USER = "ignis.discovery.etcd.user";
    public static final String DISCOVERY_ETCD_$PASSWORD$ = "ignis.discovery.etcd.$password$";

    public static final String CRYPTO_SECRET = "ignis.crypto.secret";
    public static final String CRYPTO_PUBLIC = "ignis.crypto.public";
    public static final String CRYPTO_$PRIVATE$ = "ignis.crypto.$private$";

    public static final String EXECUTOR = "ignis.executor";
    public static final String EXECUTOR_NAME = "ignis.executor.name";
    public static final String EXECUTOR_IMAGE = "ignis.executor.image";
    public static final String EXECUTOR_INSTANCES = "ignis.executor.instances";
    public static final String EXECUTOR_CORES = "ignis.executor.cores";
    public static final String EXECUTOR_MEMORY = "ignis.executor.memory";
    public static final String EXECUTOR_GPU = "ignis.executor.gpu";
    public static final String EXECUTOR_NODE = "ignis.executor.node";
    public static final String EXECUTOR_NODELIST = "ignis.executor.nodelist";
    public static final String EXECUTOR_PORTS = "ignis.executor.ports";
    public static final String EXECUTOR_BINDS = "ignis.executor.binds";
    public static final String EXECUTOR_ENV = "ignis.executor.env";
    public static final String EXECUTOR_ISOLATION = "ignis.executor.isolation";

    public static final String HEALTHCHECK_PORT = "ignis.healthcheck.port";
    public static final String HEALTHCHECK_URL = "ignis.healthcheck.url";
    public static final String HEALTHCHECK_INTERVAL = "ignis.healthcheck.interval";
    public static final String HEALTHCHECK_TIMEOUT = "ignis.healthcheck.timeout";
    public static final String HEALTHCHECK_RETRIES = "ignis.healthcheck.retries";
    public static final String HEALTHCHECK_DISABLE = "ignis.healthcheck.disable";

    public static final String JOB_ID = "ignis.job.id";
    public static final String JOB_CLUSTER = "ignis.job.cluster";
    public static final String JOB_SOCKETS = "ignis.job.sockets";
    public static final String JOB_DIR = "ignis.job.dir";
    public static final String JOB_CONTAINER_ID = "ignis.job.container.id";
    public static final String JOB_CONTAINER_DIR = "ignis.job.container.dir";
    public static final String JOB_EXECUTOR_ID = "ignis.job.executor.id";
    public static final String JOB_EXECUTOR_DIR = "ignis.job.executor.dir";

    public static final String PARTITION_TYPE = "ignis.partition.type";
    public static final String PARTITION_MINIMAL = "ignis.partition.minimal";
    public static final String PARTITION_COMPRESSION = "ignis.partition.compression";
    public static final String PARTITION_ENCODING = "ignis.partition.encoding";

    public static final String TRANSPORT_CORES = "ignis.transport.cores";
    public static final String TRANSPORT_COMPRESSION = "ignis.transport.compression";
    public static final String TRANSPORT_PORTS = "ignis.transport.ports";
    public static final String TRANSPORT_TYPE = "ignis.transport.type";
    public static final String TRANSPORT_MINIMAL = "ignis.transport.minimal";

    public static final String MODULES_IO_COMPRESSION = "ignis.modules.io.compression";
    public static final String MODULES_IO_CORES = "ignis.modules.io.cores";
    public static final String MODULES_IO_OVERWRITE = "ignis.modules.io.overwrite";
    public static final String MODULES_SORT_SAMPLES = "ignis.modules.sort.samples";
    public static final String MODULES_SORT_RESAMPLING = "ignis.modules.sort.resampling";
    public static final String MODULES_EXCHANGE_TYPE = "ignis.modules.exchange.type";
    public static final String MODULES_RECOVERY_ATTEMPS = "ignis.modules.recovery.attempts";

    public static final String SCHEDULER_URL = "ignis.scheduler.url";
    public static final String SCHEDULER_NAME = "ignis.scheduler.name";
    public static final String SCHEDULER_ENV = "ignis.scheduler.env";
    public static final String SCHEDULER_PARAM = "ignis.scheduler.param";
    public static final String SUBMITTER_BINDS = "ignis.submitter.binds";

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
