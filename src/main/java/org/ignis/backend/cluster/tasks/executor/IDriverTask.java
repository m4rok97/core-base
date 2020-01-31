/*
 * Copyright (C) 2019 César Pomar
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IExecutorExceptionWrapper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IKeys;
import org.ignis.rpc.IExecutorException;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public abstract class IDriverTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ICountTask.class);

    public static class Shared {

        public Shared(int executors) {
            this.executors = executors;
            barrier = new IBarrier(executors + 1);//executors + driver
        }

        protected final IBarrier barrier;
        protected final int executors;
        private String group;
        private boolean flag;
        private List<List<ByteBuffer>> buffer;

    }

    protected final Shared shared;
    protected final boolean driver;
    protected final String id;
    private final long dataId;
    private final ISource src;

    protected IDriverTask(String name, IExecutor executor, Shared shared, boolean driver, long dataId, ISource src) {
        super(name, executor, !driver ? Mode.LOAD : Mode.NONE);
        this.shared = shared;
        this.driver = driver;
        this.dataId = dataId;
        this.src = src;
        this.id = executor.getContainer().getCluster() + "-" + executor.getWorker();
    }

    protected IDriverTask(String name, IExecutor executor, Shared shared, boolean driver, ISource src) {
        this(name, executor, shared, driver, 0, src);
    }

    protected IDriverTask(String name, IExecutor executor, Shared shared, boolean driver, long dataId) {
        this(name, executor, shared, driver, dataId, null);
    }

    private void driverConnection(ITaskContext context) throws IgnisException {
        if (executor.getTransport().isOpen()) {
            try {
                executor.getExecutorServerModule().test();
                return;
            } catch (Exception ex) {
                LOGGER.warn(log() + "driver conection lost");
            }
        }
        LOGGER.warn(log() + "connecting to the driver");

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
            executor.getExecutorServerModule().start(executor.getExecutorProperties());
        } catch (IExecutorException ex) {
            throw new IExecutorExceptionWrapper(ex);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    private void mpiDriverGroup(ITaskContext context) throws IgnisException, BrokenBarrierException {
        LOGGER.info(log() + "Testing mpi driver group");
        try {
            if (driver) {
                driverConnection(context);
                shared.flag = true;
            }
            shared.barrier.await();
            if (!executor.getCommModule().hasGroup(id)) {
                shared.flag = false;
            }
            shared.barrier.await();
            if (!shared.flag) {
                if (!driver && executor.getId() == 0) {
                    LOGGER.info(log() + "Mpi driver group not found, creating a new one");
                    shared.group = executor.getCommModule().createGroup();
                }
                shared.barrier.await();
                executor.getCommModule().joinToGroup(shared.group, id);
                shared.barrier.await();
            }
        } catch (IExecutorException ex) {
            shared.barrier.fails();
            throw new IExecutorExceptionWrapper(ex);
        } catch (BrokenBarrierException ex) {
            throw ex;
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "Mpi driver group ready");
    }

    protected void mpiGather(ITaskContext context, boolean zero) throws IgnisException, BrokenBarrierException {
        LOGGER.info(log() + "Executing mpiGather");
        mpiDriverGroup(context);
        try {
            if (zero && executor.getId() == 0) { //Only Driver and executor 0 has id==0
                executor.getCommModule().driverGather0(id, src);
            } else {
                executor.getCommModule().driverGather(id, src);
            }
        } catch (IExecutorException ex) {
            shared.barrier.fails();
            throw new IExecutorExceptionWrapper(ex);
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "MpiGather executed");
    }

    protected void mpiScatter(ITaskContext context) throws IgnisException, BrokenBarrierException {
        LOGGER.info(log() + "Executing mpiScatter");
        mpiDriverGroup(context);
        try {
            if (src != null) {
                executor.getCommModule().driverScatter3(id, dataId, src);
            } else {
                executor.getCommModule().driverScatter(id, dataId);
            }
        } catch (IExecutorException ex) {
            shared.barrier.fails();
            throw new IExecutorExceptionWrapper(ex);
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "MpiScatter executed");
    }

    protected void rpcGather(ITaskContext context) throws IgnisException, BrokenBarrierException {
        LOGGER.info(log() + "Executing rpcGather");
        try {
            if (driver) {
                driverConnection(context);
                shared.buffer = new ArrayList<>(Collections.nCopies(shared.executors, null));
            }
            shared.barrier.await();
            if (!driver) {
                shared.buffer.set((int) executor.getId(), executor.getCommModule().getPartitions());
            }
            shared.barrier.await();
            List<ByteBuffer> partitions = shared.buffer.stream().flatMap(x -> x.stream()).collect(Collectors.toList());
            if (driver) {
                executor.getCommModule().setPartitions(partitions);
                context.saveContext(executor);
                context.set("result", context.contextStack(executor).get(0));
            }
            shared.barrier.await();
        } catch (IExecutorException ex) {
            shared.barrier.fails();
            shared.buffer = null;
            throw new IExecutorExceptionWrapper(ex);
        } catch (BrokenBarrierException ex) {
            throw ex;
        } catch (Exception ex) {
            shared.barrier.fails();
            shared.buffer = null;
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "RpcGather executed");
    }

    protected void rpcScatter(ITaskContext context) throws IgnisException, BrokenBarrierException {
        LOGGER.info(log() + "Executing rpcScatter");
        try {
            if (driver) {
                driverConnection(context);
                shared.buffer = new ArrayList<>();
            }
            shared.barrier.await();
            if (driver) {
                List<ByteBuffer> partitions = executor.getCommModule().getPartitions();
                int executorPartitions = partitions.size() / shared.executors;
                int remainder = partitions.size() % shared.executors;
                int init = 0;
                for (int i = 0; i < shared.executors; i++) {
                    int end = init + executorPartitions;
                    if (remainder < i) {
                        end++;
                    }
                    partitions.addAll(partitions.subList(init, end));
                    init = end;
                }
                shared.buffer.add(executor.getCommModule().getPartitions());
            }
            shared.barrier.await();
            if (!driver) {
                if (src != null) {
                    executor.getCommModule().setPartitions2(shared.buffer.get((int) executor.getId()), src);
                } else {
                    executor.getCommModule().setPartitions(shared.buffer.get((int) executor.getId()));
                }
            }
            shared.barrier.await();
        } catch (IExecutorException ex) {
            shared.barrier.fails();
            throw new IExecutorExceptionWrapper(ex);
        } catch (BrokenBarrierException ex) {
            throw ex;
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "RpcScatter executed");
    }

    protected boolean isTransportMinimal(ITaskContext context) throws IgnisException, BrokenBarrierException {
        try {
            if (!driver && executor.getIoModule().partitionApproxSize() > executor.getProperties().getLong(IKeys.TRANSPORT_MINIMAL)) {
                shared.flag = false;
            }
            shared.barrier.await();
            return shared.flag;
        } catch (IExecutorException ex) {
            shared.barrier.fails();
            throw new IExecutorExceptionWrapper(ex);
        } catch (BrokenBarrierException ex) {
            throw ex;
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    protected void gather(ITaskContext context) throws IgnisException, BrokenBarrierException {
        if (isTransportMinimal(context)) {
            rpcGather(context);
        } else {
            mpiGather(context, false);
        }
    }

    protected void gather(ITaskContext context, boolean zero) throws IgnisException, BrokenBarrierException {
        if (isTransportMinimal(context)) {
            rpcGather(context);
        } else {
            mpiGather(context, zero);
        }
    }

    protected void scatter(ITaskContext context) throws IgnisException, BrokenBarrierException {
        if (isTransportMinimal(context)) {
            rpcScatter(context);
        } else {
            mpiScatter(context);
        }
    }

}
