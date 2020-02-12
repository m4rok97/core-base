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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IExecutorExceptionWrapper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.scheduler.model.IPort;
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
        boolean running = false;
        if (executor.getTransport().isOpen()) {
            try {
                executor.getExecutorServerModule().test();
                LOGGER.info(log() + "Executor already running");
                return;
            } catch (TException ex) {
                LOGGER.info(log() + "Executor connection lost, testing executor process");
                String check = executor.getContainer().getTunnel().execute("ps -p " + executor.getPid() + " > /dev/null && echo $?", false);
                running = check.startsWith("0");
                if (running) {
                    LOGGER.info(log() + "Executor process is alive, reconnecting");
                } else {
                    LOGGER.warn(log() + "Executor dead " + ex);
                }
            }

        }
        if (!running) {
            LOGGER.info(log() + "Starting new executor");
            StringBuilder startScript = new StringBuilder();
            startScript.append("export MPICH_STATIC_PORTS='");
            int mpiMaxPorts = executor.getProperties().getInteger(IKeys.TRANSPORT_PORTS);
            List<IPort> mpiPorts = executor.getContainer().getInfo().getPorts().subList(0, mpiMaxPorts);
            startScript.append(mpiPorts.stream().map((IPort p) -> String.valueOf(p.getContainerPort())).collect(Collectors.joining(" ")));
            startScript.append("'\n");

            startScript.append("export IGNIS_WORKING_DIRECTORY='");
            startScript.append(executor.getProperties().getString(IKeys.WORKING_DIRECTORY));
            startScript.append("'\n");

            startScript.append("nohup ignis-run ");
            startScript.append("ignis-").append(type).append(' ');
            startScript.append(executor.getContainer().getTunnel().getRemotePort(executor.getPort())).append(' ');
            startScript.append(executor.getProperties().getInteger(IKeys.EXECUTOR_RPC_COMPRESSION)).append(' ');
            if (executor.getProperties().getString(IKeys.SCHEDULER_CONTAINER).equals("docker")) {
                /*Redirect to docker log */
                startScript.append("> /proc/1/fd/1 2> /proc/1/fd/2 ");
            }
            startScript.append("& \n");
            startScript.append("printf $!");/*get PID*/

            String output = executor.getContainer().getTunnel().execute(startScript.toString(), false);
            executor.setPid(Integer.parseInt(output));
        }

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

        try {
            executor.getExecutorServerModule().test();
            if (running) {
                return;
            }
            Map<String, String> executorProperties = executor.getExecutorProperties();
            if (Boolean.getBoolean(IKeys.DEBUG)) {
                StringBuilder writer = new StringBuilder();
                for (Map.Entry<String, String> entry : executorProperties.entrySet()) {
                    writer.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
                }
                LOGGER.info("Debug:" + log() + " ExecutorProperties{\n" + writer.toString() + '}');
            }
            executor.getExecutorServerModule().start(executorProperties);
        } catch (IExecutorException ex) {
            throw new IExecutorExceptionWrapper(ex);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

}
