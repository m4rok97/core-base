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

import org.apache.thrift.TException;
import org.ignis.backend.exception.IExecutorExceptionWrapper;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.IExecutorException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author César Pomar
 */
public class ITaskContext {

    private final Map<IExecutor, List<Long>> contexts;
    private final Map<String, Object> vars;
    private final List<ITaskContext> subContexts;

    public ITaskContext() {
        contexts = new ConcurrentHashMap<>();
        vars = new ConcurrentHashMap<>();
        subContexts = new ArrayList<>();
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

    public List<Long> contextStack(IExecutor e) {
        List<Long> executorContext = contexts.get(e);
        if (executorContext != null) {
            return Collections.unmodifiableList(executorContext);
        }
        return null;
    }

    public long popContext(IExecutor e) throws IgnisException {
        List<Long> executorContext = contexts.get(e);

        if (executorContext == null || executorContext.isEmpty()) {
            throw new IgnisException("Executor context error");
        }

        long id = executorContext.get(executorContext.size() - 1);
        executorContext.remove(executorContext.size() - 1);
        return id;
    }

    public void loadContext(IExecutor e) throws IgnisException {
        try {
            e.getCacheContextModule().loadContext(popContext(e));
        } catch (IExecutorException ex) {
            throw new IExecutorExceptionWrapper(ex);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    public void loadContextAsVariable(IExecutor e, String name) throws IgnisException {
        try {
            e.getCacheContextModule().loadContextAsVariable(popContext(e), name);
        } catch (IExecutorException ex) {
            throw new IExecutorExceptionWrapper(ex);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    public void clearContext(IExecutor e) throws IgnisException {
        try {
            e.getCacheContextModule().clearContext();
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

    public ITaskContext newSubContext() {
        ITaskContext sc = new ITaskContext();
        sc.vars.putAll(this.vars);
        subContexts.add(sc);
        return sc;
    }

    public void mergeSubContexts() {
        for (ITaskContext sc : subContexts) {
            this.vars.putAll(sc.vars);
            for (var entry : sc.contexts.entrySet()) {
                if (this.contexts.containsKey(entry.getKey())) {
                    this.contexts.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    this.contexts.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

}
