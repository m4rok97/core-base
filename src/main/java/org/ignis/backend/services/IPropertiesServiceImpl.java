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
import org.apache.thrift.TException;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.IRemoteException;
import org.ignis.rpc.driver.IPropertiesService;

/**
 *
 * @author César Pomar
 */
public final class IPropertiesServiceImpl extends IService implements IPropertiesService.Iface {

    public IPropertiesServiceImpl(IAttributes attributes) {
        super(attributes);
    }

    @Override
    public long newInstance() throws TException {
        synchronized (attributes.defaultProperties) {
            return attributes.addProperties(new IProperties(attributes.defaultProperties));
        }
    }

    @Override
    public long newInstance2(long id) throws TException {
        IProperties properties = attributes.getProperties(id);
        synchronized (properties) {
            return attributes.addProperties(new IProperties(properties));
        }
    }

    @Override
    public String setProperty(long id, String key, String value) throws IRemoteException, TException {
        IProperties properties = attributes.getProperties(id);
        synchronized (properties) {
            return properties.setProperty(key, value);
        }
    }

    @Override
    public String getProperty(long id, String key) throws IRemoteException, TException {
        IProperties properties = attributes.getProperties(id);
        synchronized (properties) {
            return properties.getProperty(key);
        }
    }

    @Override
    public boolean isProperty(long id, String key) throws IRemoteException, TException {
        IProperties properties = attributes.getProperties(id);
        synchronized (properties) {
            return properties.isProperty(key);
        }
    }

    @Override
    public Map<String, String> toMap(long id) throws IRemoteException, TException {
        IProperties properties = attributes.getProperties(id);
        synchronized (properties) {
            return properties.toMap();
        }
    }

    @Override
    public void fromMap(long id, Map<String, String> _map) throws IRemoteException, TException {
        IProperties properties = attributes.getProperties(id);
        synchronized (properties) {
            properties.fromMap(_map);
        }
    }

    @Override
    public void toFile(long id, String path) throws IRemoteException, TException {
        IProperties properties = attributes.getProperties(id);
        synchronized (properties) {
            properties.toFile(path);
        }
    }

    @Override
    public void fromFile(long id, String path) throws IRemoteException, TException {
        IProperties properties = attributes.getProperties(id);
        synchronized (properties) {
            properties.fromFile(path);
        }
    }

    @Override
    public void reset(long id) throws IRemoteException, TException {
        IProperties properties = attributes.getProperties(id);
        synchronized (properties) {
            properties.reset(attributes.defaultProperties);
        }
    }

}
