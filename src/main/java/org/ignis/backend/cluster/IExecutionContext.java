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
import org.ignis.backend.exception.IgnisException;

/**
 *
 * @author César Pomar
 */
public class IExecutionContext {

    private final static Map<IExecutor, List<Long>> INDEXS = new ConcurrentHashMap<>();
    private final Map<IExecutor, List<Long>> contexts;
    private final Map map;

    public IExecutionContext() {
        contexts = new ConcurrentHashMap<>();
        map = new ConcurrentHashMap<>();
    }

    public void saveContext(IExecutor e) throws IgnisException {
        List<Long> ctx = contexts.get(e);
        List<Long> ids = INDEXS.get(e);
        long id;
        if (ctx == null) {
            ctx = new ArrayList<>();
            contexts.put(e, ctx);
        }
        if (ids == null) {
            ids = new ArrayList<>();
            INDEXS.put(e, ids);
        }
        if (ids.isEmpty()) {
            id = 0;
        } else {
            id = ids.get(ids.size() - 1) + 1;
        }
        ctx.add(id);
        ids.add(id);
        try {
            e.getStorageModule().saveContext(id);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    public void loadContext(IExecutor e) throws IgnisException {
        List<Long> ctx = contexts.get(e);
        List<Long> ids = INDEXS.get(e);
        if (ctx == null || ctx.isEmpty()) {
            throw new IgnisException("Executor context error");
        }
        long id = ctx.remove(0);
        ids.remove((Long) id);
        try {
            e.getStorageModule().loadContext(id);
        } catch (TException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    public <V> void set(Object key, V value) {
        map.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public <V> V get(Object key) {
        return (V)map.get(key);
    }

}
