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

import org.apache.thrift.TException;
import org.ignis.backend.exception.IDriverExceptionImpl;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.driver.IDriverException;
import org.ignis.rpc.driver.IPropertiesService;

import java.util.Map;

/**
 * @author César Pomar
 */
public final class IPropertiesServiceImpl extends IService implements IPropertiesService.Iface {

    public IPropertiesServiceImpl(IAttributes attributes) {
        super(attributes);
    }

    @Override
    public long newInstance() throws IDriverException, TException {
        try {
            return attributes.addProperties(new IProperties(attributes.defaultProperties));
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public long newInstance2(long id) throws IDriverException, TException {
        try {
            IProperties source = attributes.getProperties(id);
            IProperties properties;
            synchronized (source) {
                properties = source.copy();
            }
            return attributes.addProperties(properties);
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public String setProperty(long id, String key, String value) throws IDriverException, TException {
        try {
            IProperties properties = attributes.getProperties(id);
            synchronized (properties) {
                return properties.setProperty(key, value);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public String getProperty(long id, String key) throws IDriverException, TException {
        try {
            IProperties properties = attributes.getProperties(id);
            synchronized (properties) {
                return properties.getProperty(key);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public String rmProperty(long id, String key) throws IDriverException, TException {
        try {
            IProperties properties = attributes.getProperties(id);
            synchronized (properties) {
                return properties.rmProperty(key);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public boolean contains(long id, String key) throws IDriverException, TException {
        try {
            IProperties properties = attributes.getProperties(id);
            synchronized (properties) {
                return properties.contains(key);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public Map<String, String> toMap(long id, boolean defaults) throws IDriverException, TException {
        try {
            IProperties properties = attributes.getProperties(id);
            synchronized (properties) {
                return properties.toMap(defaults);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void fromMap(long id, Map<String, String> _map) throws IDriverException, TException {
        try {
            IProperties properties = attributes.getProperties(id);
            synchronized (properties) {
                properties.fromMap(_map);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void load(long id, String path) throws IDriverException, TException {
        try {
            IProperties properties = attributes.getProperties(id);
            synchronized (properties) {
                properties.load(path);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void store(long id, String path) throws IDriverException, TException {
        try {
            IProperties properties = attributes.getProperties(id);
            synchronized (properties) {
                properties.store(path);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void clear(long id) throws IDriverException, TException {
        try {
            IProperties properties = attributes.getProperties(id);
            synchronized (properties) {
                properties.clear();
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }
}
