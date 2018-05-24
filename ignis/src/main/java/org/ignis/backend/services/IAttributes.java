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
package org.ignis.backend.services;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;

/**
 *
 * @author César Pomar
 */
public class IAttributes {

    public final IProperties defaultProperties;
    public final AtomicLong idClusterGen;
    public final AtomicLong idPropertiesGen;
    public final Map<Long, ICluster> clusters;
    public final Map<Long, IProperties> properties;

    public IAttributes() {
        this.defaultProperties = new IProperties();
        this.clusters = new ConcurrentHashMap<>();
        this.properties = new ConcurrentHashMap<>();
        this.idClusterGen = new AtomicLong();
        this.idPropertiesGen = new AtomicLong();
    }

    public IProperties getProperties(long id) throws IgnisException {
        IProperties prop = properties.get(id);
        if (prop == null) {
            throw new IgnisException("Properties doesn't exist");
        }
        return prop;
    }

    public ICluster getCluster(long id) throws IgnisException {
        ICluster cluster = clusters.get(id);
        if (cluster == null) {
            throw new IgnisException("Properties doesn't exist");
        }
        return cluster;
    }

}
