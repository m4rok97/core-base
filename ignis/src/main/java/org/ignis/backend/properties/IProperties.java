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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.ignis.backend.exception.IgnisException;

/**
 *
 * @author César Pomar
 */
public class IProperties {

    private Map<String, String> properties;

    public IProperties() {
        properties = new HashMap<>();
    }

    public String setProperty(String key, String value) {
        if (value == null) {
            value = "";
        }
        return properties.put(key, value);
    }

    public String getProperty(String key) {
        String value = properties.get(key);
        if (value == null) {
            return "";
        }
        return value;
    }

    public boolean isProperty(String key) {
        return properties.containsKey(key);
    }

    public Map<String, String> toMap() {
        return Collections.unmodifiableMap(properties);
    }

    public void fromMap(Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            setProperty(entry.getKey(), entry.getValue());
        }
    }

    public void toFile(String path) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void fromFile(String path) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
