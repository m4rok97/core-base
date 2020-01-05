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

import java.util.concurrent.BrokenBarrierException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class ITreeAggregateTask extends IDriverTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ITreeAggregateTask.class);

    private final ISource seqOp;
    private final ISource combOp;
    private final long depth;

    public ITreeAggregateTask(String name, IExecutor executor, Shared shared, boolean driver, ISource seqOp, ISource combOp) {
        this(name, executor, shared, driver, seqOp, combOp, 2);
    }
    
    public ITreeAggregateTask(String name, IExecutor executor, Shared shared, boolean driver, ISource seqOp, ISource combOp, long depth) {
        super(name, executor, shared, driver);
        this.seqOp = seqOp;
        this.combOp = combOp;
        this.depth = depth;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        LOGGER.info(log() + "Executing treeAggregate");
        try {
            if (!driver) {
                executor.getGeneralActionModule().treeAggregate(seqOp, combOp, depth);
            }
            shared.barrier.await();
            gather(context, true);
        } catch (IgnisException ex) {
            shared.barrier.fails();
            throw ex;
        } catch (BrokenBarrierException ex) {
            //Other Task has failed
        } catch (Exception ex) {
            shared.barrier.fails();
            throw new IgnisException(ex.getMessage(), ex);
        }
        LOGGER.info(log() + "TreeAggregate executed");
    }

}
