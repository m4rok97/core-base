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
package org.ignis.backend.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.thrift.TException;
import org.ignis.backend.exception.IExecutorExceptionWrapper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.IExecutorException;

/**
 *
 * @author César Pomar
 */
public class ITaskContext {

    private final Map<IExecutor, List<Long>> contexts;
    private final Map<String, Object> vars;

    public ITaskContext() {
        contexts = new ConcurrentHashMap<>();
        vars = new ConcurrentHashMap<>();
    }

    public void saveContext(IExecutor e) throws IgnisException {
        List<Long> executorContext = contexts.get(e);
        if (executorContext == null) {
            contexts.put(e, executorContext = new ArrayList<>());
        }

        try {
            executorContext.add(e.getCacheContextModule().saveContext());
        } catch (IExecutorException ex) {
            throw new IExecutorExceptionWrapper(ex);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    public void loadContext(IExecutor e) throws IgnisException {
        List<Long> executorContext = contexts.get(e);

        if (executorContext == null || executorContext.isEmpty()) {
            throw new IgnisException("Executor context error");
        }

        try {
            e.getCacheContextModule().loadContext(executorContext.get(executorContext.size() - 1));
            executorContext.remove(executorContext.size() - 1);
        } catch (IExecutorException ex) {
            throw new IExecutorExceptionWrapper(ex);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    public void set(String key, Object value) {
        vars.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public <V> V get(String key) {
        return (V) vars.get(key);
    }

}
