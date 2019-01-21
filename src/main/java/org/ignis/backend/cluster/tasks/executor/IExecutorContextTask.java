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

import org.ignis.backend.cluster.IExecutionContext;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.helpers.IHelper;
import org.ignis.backend.exception.IgnisException;

/**
 *
 * @author César Pomar
 */
public abstract class IExecutorContextTask extends IExecutorTask {

    public enum Mode {
        LOAD, SAVE, LOAD_AND_SAVE
    }

    private final Mode mode;

    public IExecutorContextTask(IHelper helper, IExecutor executor, Mode mode) {
        super(helper, executor);
        this.mode = mode;
    }

    @Override
    public final void loadContext(IExecutionContext context) throws IgnisException {
        if (mode == Mode.LOAD || mode == Mode.LOAD_AND_SAVE) {
            context.loadContext(executor);
        }
    }

    @Override
    public final void saveContext(IExecutionContext context) throws IgnisException {
        if (mode == Mode.SAVE || mode == Mode.LOAD_AND_SAVE) {
            context.saveContext(executor);
        }
    }

}
