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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author César Pomar
 */
public abstract class IDriverTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ICountTask.class);

    public static class Shared {

        public Shared(int executors) {
            this.executors = executors;
            barrier = new IBarrier(executors + 1);//executors + driver
            value = new AtomicLong();
        }

        protected final IBarrier barrier;
        protected final int executors;
        private String group;
        private byte protocol;
        private boolean flag;
        private final AtomicLong value;
        private List<List<ByteBuffer>> buffer;

    }

    protected final Shared shared;
    protected final boolean driver;
    private final Long dataId;
    private final ISource src;
    private Integer attempt;

    protected IDriverTask(String name, IExecutor executor, Shared shared, boolean driver, Long dataId, ISource src) {
        super(name, executor, dataId != null ? Mode.SAVE : Mode.LOAD);
        this.shared = shared;
        this.driver = driver;
        this.dataId = dataId;
        this.src = src;
        this.attempt = -1;
    }

    protected IDriverTask(String name, IExecutor executor, Shared shared, boolean driver, ISource src) {
        this(name, executor, shared, driver, null, src);
    }

    protected IDriverTask(String name, IExecutor executor, Shared shared, boolean driver, long dataId) {
        this(name, executor, shared, driver, dataId, null);
    }

    private void driverConnection(ITaskContext context) throws IgnisException {
        if (executor.isConnected()) {
            try {
                executor.getExecutorServerModule().test();
                return;
            } catch (Exception ex) {
                LOGGER.warn(log() + "driver conection lost");
            }
        }
        LOGGER.warn(log() + "connecting to the driver");

        for (int i = 0; i < 300; i++) {
            try {
                executor.connect();
                break;
            } catch (TException ex) {
                if (i == 299) {
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
            executor.getExecutorServerModule().start(executor.getExecutorProperties());
        } catch (IExecutorException ex) {
            throw new IExecutorExceptionWrapper(ex);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    private String mpiDriverGroup(ITaskContext context) throws IgnisException, BrokenBarrierException {
        LOGGER.info(log() + "Testing mpi driver group");
        String id;
        try {
            if (driver) {
                driverConnection(context);
                shared.flag = true;
            }
            if (!driver && executor.getId() == 0) {
                shared.group = executor.getContainer().getCluster() + "." + executor.getWorker();
            }
            shared.barrier.await();
            id = shared.group;
            if (!executor.getCommModule().hasGroup(id) || executor.getResets() != attempt) {
                shared.flag = false;
            }
            shared.barrier.await();
            if (!shared.flag) {
                if (attempt != -1) {
                    executor.getCommModule().destroyGroup(id);
                }
                attempt = executor.getResets();
                if (!driver && executor.getId() == 0) {
                    LOGGER.info(log() + "Mpi driver group not found, creating a new one");
                    shared.group = executor.getCommModule().openGroup();
                }
                shared.barrier.await();
                executor.getCommModule().joinToGroupName(shared.group, !driver, id);
                shared.barrier.await();
                if (!driver && executor.getId() == 0) {
                    executor.getCommModule().closeGroup();
                }
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
        return id;
    }

    protected void mpiGather(ITaskContext context, boolean zero) throws IgnisException, BrokenBarrierException {
        LOGGER.info(log() + "Executing mpiGather");
        String id = mpiDriverGroup(context);
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

    protected void mpiScatter(ITaskContext context, long partitions) throws IgnisException, BrokenBarrierException {
        LOGGER.info(log() + "Executing mpiScatter");
        String id = mpiDriverGroup(context);
        try {
            if (driver) {
                executor.getCacheContextModule().loadCache(dataId);
            }
            shared.barrier.await();
            if (src != null) {
                executor.getCommModule().driverScatter3(id, partitions, src);
            } else {
                executor.getCommModule().driverScatter(id, partitions);
            }
            shared.barrier.await();
            if (driver) {
                context.saveContext(executor);
                context.set("result", context.popContext(executor));
            }
            shared.barrier.await();
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
                shared.protocol = executor.getCommModule().getProtocol();
                shared.buffer = new ArrayList<>(Collections.nCopies(shared.executors, null));
            }
            shared.barrier.await();
            if (!driver) {
                shared.buffer.set((int) executor.getId(), executor.getCommModule().getPartitions(shared.protocol));
            }
            shared.barrier.await();
            if (driver) {
                List<ByteBuffer> group = shared.buffer.stream().flatMap(x -> x.stream()).collect(Collectors.toList());
                executor.getCommModule().setPartitions2(group, src);
                context.saveContext(executor);
                context.set("result", context.popContext(executor));
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

    protected void rpcScatter(ITaskContext context, long partitions) throws IgnisException, BrokenBarrierException {
        LOGGER.info(log() + "Executing rpcScatter");
        try {
            if (driver) {
                driverConnection(context);
                executor.getCacheContextModule().loadCache(dataId);
                shared.buffer = new ArrayList<>();
            } else if (executor.getId() == 0) {
                shared.protocol = executor.getCommModule().getProtocol();
            }
            shared.barrier.await();
            if (driver) {
                List<ByteBuffer> group = executor.getCommModule().getPartitions2(shared.protocol, partitions);
                int executorPartitions = group.size() / shared.executors;
                int remainder = group.size() % shared.executors;
                int init = 0;
                for (int i = 0; i < shared.executors; i++) {
                    int end = init + executorPartitions;
                    if (remainder < i) {
                        end++;
                    }
                    shared.buffer.add(group.subList(init, end));
                    init = end;
                }
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

    protected boolean isTransportMinimal(ITaskContext context, boolean toDriver) throws IgnisException, BrokenBarrierException {
        try {
            if (driver) {
                shared.value.set(0);
            }
            shared.barrier.await();
            if (toDriver && !driver) {
                shared.value.addAndGet(executor.getIoModule().partitionApproxSize());
            } else if (!toDriver && driver) {
                shared.value.addAndGet(executor.getIoModule().partitionApproxSize());
            }
            shared.barrier.await();
            return shared.value.get() < executor.getProperties().getLong(IKeys.TRANSPORT_MINIMAL);
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
        gather(context, false);
    }

    protected void gather(ITaskContext context, boolean zero) throws IgnisException, BrokenBarrierException {
        if (isTransportMinimal(context, true)) {
            rpcGather(context);
        } else {
            mpiGather(context, zero);
        }
    }

    protected void scatter(ITaskContext context, long partitions) throws IgnisException, BrokenBarrierException {
        if (isTransportMinimal(context, false)) {
            rpcScatter(context, partitions);
        } else {
            mpiScatter(context, partitions);
        }
    }

}
