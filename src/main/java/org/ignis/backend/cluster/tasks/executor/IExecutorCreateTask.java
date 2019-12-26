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
package org.ignis.backend.cluster.tasks.executor;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.exception.IExecutorExceptionWrapper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IKeys;
import org.ignis.rpc.IExecutorException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IExecutorCreateTask extends IExecutorTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IExecutorCreateTask.class);

    private final String type;
    private final int cores;

    public IExecutorCreateTask(String name, IExecutor executor, String type, int cores) {
        super(name, executor);
        this.type = type;
        this.cores = cores;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        if (executor.getTransport().isOpen()) {
            try {
                executor.getExecutorServerModule().test();
                LOGGER.info(log() + "Executor already running");
                return;
            } catch (TException ex) {
                LOGGER.warn(log() + "Executor dead " + ex);
            }

        }
        LOGGER.info(log() + "Starting new executor");
        StringBuilder startScript = new StringBuilder();
        startScript.append("#!/bin/bash\n");

        startScript.append("export MPICH_STATIC_PORTS='");
        int mpiMaxPorts = executor.getProperties().getInteger(IKeys.TRANSPORT_PORTS);
        List<Integer> tcpPorts = executor.getContainer().getInfo().getNetwork().getTcpPorts();
        List<Integer> mpiPorts = tcpPorts.subList(tcpPorts.size() - mpiMaxPorts, tcpPorts.size());
        startScript.append(mpiPorts.stream().map(String::valueOf).collect(Collectors.joining(" ")));
        startScript.append("'\n");

        startScript.append("nohup ignis-executor ");
        startScript.append(type).append(' ');
        startScript.append(executor.getPort()).append(' ');
        startScript.append(executor.getProperties().getInteger(IKeys.EXECUTOR_RPC_COMPRESSION)).append(' ');
        if (executor.getProperties().getString(IKeys.SCHEDULER_CONTAINER).equals("docker")) {
            /*Redirect to docker log */
            startScript.append("> /proc/1/fd/1 2> /proc/1/fd/2");
        }
        startScript.append("& \n");
        startScript.append("echo $!");/*get PID*/

        String output = executor.getContainer().getTunnel().execute(startScript.toString());

        for (int i = 0; i < 10; i++) {
            try {
                executor.getTransport().open();
                break;
            } catch (TException ex) {
                if (i == 9) {
                    throw new IgnisException(ex.getMessage(), ex);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex1) {
                    throw new IgnisException(ex.getMessage(), ex);
                }
            }
        }

        executor.setPid(Integer.parseInt(output));
        try {
            executor.getExecutorServerModule().updateProperties(executor.getProperties().toMap());
        } catch (IExecutorException ex) {
            throw new IExecutorExceptionWrapper(ex);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

}
