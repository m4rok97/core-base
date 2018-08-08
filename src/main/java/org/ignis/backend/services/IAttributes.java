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
    private final AtomicLong idClusterGen;
    private final AtomicLong idPropertiesGen;
    private final Map<Long, ICluster> clustersMap;
    private final Map<Long, IProperties> propertiesMap;

    public IAttributes() {
        this.defaultProperties = new IProperties();
        this.clustersMap = new ConcurrentHashMap<>();
        this.propertiesMap = new ConcurrentHashMap<>();
        this.idClusterGen = new AtomicLong();
        this.idPropertiesGen = new AtomicLong();
    }

    public IProperties getProperties(long id) throws IgnisException {
        IProperties properties = propertiesMap.get(id);
        if (properties == null) {
            throw new IgnisException("Properties doesn't exist");
        }
        return properties;
    }

    public long addProperties(IProperties properties) {
        long id = idPropertiesGen.incrementAndGet();
        propertiesMap.put(id, properties);
        return id;
    }

    public ICluster getCluster(long id) throws IgnisException {
        ICluster cluster = clustersMap.get(id);
        if (cluster == null) {
            throw new IgnisException("Cluster doesn't exist");
        }
        return cluster;
    }

    public long newIdCluster() {
        return idClusterGen.incrementAndGet();
    }

    public void addCluster(ICluster cluster) {
        clustersMap.put(cluster.getId(), cluster);
    }

}
