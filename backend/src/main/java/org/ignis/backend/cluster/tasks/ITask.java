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
package org.ignis.backend.cluster.tasks;

import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IgnisException;

/**
 * @author César Pomar
 */
public abstract class ITask {

    protected final String name;

    public ITask(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    protected void before(ITaskContext context) throws IgnisException {
    }

    protected abstract void run(ITaskContext context) throws IgnisException;

    protected void after(ITaskContext context) throws IgnisException {
    }

    public void start(ITaskContext context) throws IgnisException {
        before(context);
        run(context);
        after(context);

    }
}
