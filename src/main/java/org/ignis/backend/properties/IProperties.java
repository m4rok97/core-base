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
package org.ignis.backend.properties;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.ignis.backend.exception.IgnisException;

/**
 *
 * @author César Pomar
 */
public final class IProperties {

    private Map<String, String> properties;

    public IProperties(IProperties defaults) {
        this();
        fromMap(defaults.properties);
    }

    public IProperties() {
        properties = new HashMap<>();
    }

    public String setProperty(String key, String value) {
        String oldValue = properties.put(fixKey(key), value == null ? "" : value);
        return oldValue == null ? "" : oldValue;
    }

    public String getProperty(String key) {
        String value = properties.get(fixKey(key));
        return value == null ? "" : value;
    }

    public String getString(String key) throws IgnisException {
        return IPropertyParser.getString(this, key);
    }

    public boolean getBoolean(String key) throws IgnisException {
        return IPropertyParser.getBoolean(this, key);
    }

    public int getInteger(String key) throws IgnisException {
        return IPropertyParser.getInteger(this, key);
    }

    public long getLong(String key) throws IgnisException {
        return IPropertyParser.getLong(this, key);
    }

    public float getFloat(String key) throws IgnisException {
        return IPropertyParser.getFloat(this, key);
    }

    public double getDouble(String key) throws IgnisException {
        return IPropertyParser.getDouble(this, key);
    }

    public long getSILong(String key) throws IgnisException {
        return IPropertyParser.getSILong(this, key);
    }

    public boolean isProperty(String key) {
        return properties.containsKey(fixKey(key));
    }

    public Map<String, String> toMap() {
        return Collections.unmodifiableMap(properties);
    }

    public void fromMap(Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            setProperty(entry.getKey(), entry.getValue());
        }
    }

    public void toFile(String path) throws IgnisException {
        try {
            toTree().save(new File(path));
        } catch (IOException ex) {
            throw new IgnisException("Fails to save properties", ex);
        }
    }

    public void fromFile(String path) throws IgnisException {
        try {
            ITreeProperties tree = new ITreeProperties();
            tree.load(new File(path));
            tree.toMap(properties);
        } catch (IOException ex) {
            throw new IgnisException("Fails to load properties", ex);
        }
    }

    public ITreeProperties.Entry getEntry(String key) {
        return toTree().getEntry(key);
    }

    public ITreeProperties.Entry getRootEntry() {
        return toTree().getRoot();
    }

    public void reset(IProperties defaults) {
        properties.clear();
        fromMap(properties);
    }

    public IProperties copy() {
        return new IProperties(this);
    }

    private String fixKey(String key) {
        if (key.startsWith("value")) {
            if (key.length() == 5) {
                return "";
            }
            key = key.substring(6);
        }
        return key.replace(".value", "");
    }

    private ITreeProperties toTree() {
        ITreeProperties tree = new ITreeProperties();
        tree.fromMap(properties);
        return tree;
    }
}
