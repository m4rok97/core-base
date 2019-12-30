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

import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class IReduceTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IReduceTask.class);

    public static class Shared {

        public Shared(int executors) {
            barrier = new IBarrier(executors);
        }

        private final IBarrier barrier;

    }

    private final Shared shared;
    private final boolean driver;

    public IReduceTask(String name, IExecutor executor, Shared shared, boolean driver, ISource src) {
        super(name, executor, Mode.LOAD);
        this.shared = shared;
        this.driver = driver;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        /*try {//TODO
            if (barrier.await() == 0) {
                shared.result.clear();
                LOGGER.info(log() + "Executing " + (ligth ? "ligth " : "") + "collect");
            }
            barrier.await();
            ByteBuffer bytes = executor.getStorageModule().collect(executor.getId(), "none", ligth);//TODO
            if (ligth) {
                shared.result.put(executor, bytes);
            }
            barrier.await();
            if (ligth) {
                ligthMode(context);
            } else {
                directMode(context);
            }
            if (barrier.await() == 0) {
                LOGGER.info(log() + "Collect executed");
            }
        } catch (IgnisException ex) {
            barrier.fails();
            throw ex;
        } catch (BrokenBarrierException ex) {
            //Other Task has failed
        } catch (Exception ex) {
            barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }*/
    }

    private void ligthMode(ITaskContext context) throws Exception {
        /*if (barrier.await() == 0) {
            context.set("result", executors.stream().map(e -> shared.result.get(e)).collect(Collectors.toList()));
        }*/
    }

    private void directMode(ITaskContext context) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");//TODO
    }

}
