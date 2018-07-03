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

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.cluster.tasks.Task;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.IFunction;

/**
 *
 * @author César Pomar
 */
public class IMapTask extends IExecutorTask {

    private final IFunction function;

    public IMapTask(IExecutor executor, IFunction function, ILock lock, Task... dependencies) {
        super(executor, lock, dependencies);
        this.function = function;
    }

    @Override
    public void execute() throws IgnisException {
        try {
            executor.getMapperModule()._map(function);
        } catch (TException ex) {
            throw new IgnisException("Map fails", ex);
        }
    }

}
