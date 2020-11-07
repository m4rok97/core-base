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
import org.ignis.backend.exception.IgnisException;

/**
 *
 * @author César Pomar
 */
public abstract class IExecutorContextTask extends IExecutorTask {

    protected enum Mode {
        LOAD, SAVE, LOAD_AND_SAVE, NONE
    }

    private final Mode mode;

    public IExecutorContextTask(String name, IExecutor executor, Mode mode) {
        super(name, executor);
        this.mode = mode;
    }

    @Override
    protected final void before(ITaskContext context) throws IgnisException {
        if (mode == Mode.LOAD || mode == Mode.LOAD_AND_SAVE) {
            context.loadContext(executor);
        }else{
            context.clearContext(executor);
        }
    }

    @Override
    protected final void after(ITaskContext context) throws IgnisException {
        if (mode == Mode.SAVE || mode == Mode.LOAD_AND_SAVE) {
            context.saveContext(executor);
        }
    }

}
