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

import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.cluster.IDriver;
import org.ignis.backend.cluster.tasks.IThreadPool;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IProperties;
import org.ignis.scheduler3.model.IContainerInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * @author César Pomar
 */
public final class IServiceStorage {

    private static final Pattern DEFAULT_CLUSTER_NAMES = Pattern.compile("Cluster\\([0-9]+\\)");

    public final IDriver driver;
    private final List<ICluster> clusterList;
    private final List<IProperties> propertiesList;
    private final IThreadPool pool;

    public IServiceStorage(IDriver driver, IThreadPool pool) {
        this.driver = driver;
        this.clusterList = new ArrayList<>();
        this.propertiesList = new ArrayList<>();
        this.pool = pool;
        addProperties(driver.getProperties());
    }

    public IProperties props() {
        return driver.getProperties();
    }

    public IDriver driver() {
        return driver;
    }

    public IThreadPool pool() {
        return pool;
    }

    public boolean isHostNetwork() {
        return driver.getInfo().network().equals(IContainerInfo.INetworkMode.HOST);
    }

    public long addProperties(IProperties properties) {
        synchronized (propertiesList) {
            propertiesList.add(properties);
            return propertiesList.size() - 1;
        }
    }

    public IProperties getProperties(long id) throws IgnisException {
        if (id < 0) {
            return getCluster(-id).getProperties();
        }
        synchronized (propertiesList) {
            if (propertiesList.size() > id) {
                return propertiesList.get((int) id);
            }
        }
        throw new IgnisException("Properties doesn't exist");
    }

    public int propertiesCount() {
        synchronized (propertiesList) {
            return propertiesList.size();
        }
    }

    public ICluster getCluster(long id) throws IgnisException {
        synchronized (clusterList) {
            if (clusterList.size() >= id || clusterList.get((int) id - 1) == null) {
                return clusterList.get((int) id - 1);
            }
        }
        throw new IgnisException("Cluster doesn't exist");
    }

    public long newCluster() {
        synchronized (clusterList) {
            clusterList.add(null);
            return clusterList.size();
        }
    }

    public void setCluster(ICluster cluster) {
        synchronized (clusterList) {
            clusterList.set((int) cluster.getId() - 1, cluster);
        }
    }

    public Collection<ICluster> getClusters() {
        synchronized (clusterList) {
            return clusterList.stream().filter(Objects::nonNull).toList();
        }
    }

    public int clustersCount() {
        synchronized (clusterList) {
            return clusterList.size();
        }
    }

}
