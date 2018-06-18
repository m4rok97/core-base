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

import java.util.regex.Pattern;
import org.ignis.backend.exception.IgnisException;

/**
 *
 * @author César Pomar
 */
public class IPropertiesParser {

    private final static Pattern BOOLEAN = Pattern.compile("y|Y|yes|Yes|YES|true|True|TRUE|on|On|ON");

    public static boolean getBoolean(IProperties prop, String key) throws IgnisException {
        return BOOLEAN.matcher(getString(prop, key)).matches();
    }

    public static String getString(IProperties prop, String key) throws IgnisException {
        if (!prop.isProperty(key)) {
            throw new IgnisException("Property " + key + " not exist");
        }
        return prop.getProperty(key);
    }

    public static long getLong(IProperties prop, String key) throws IgnisException {
        try {
            return Long.parseLong(getString(prop, key));
        } catch (NumberFormatException ex) {
            throw new IgnisException("Property " + key + " is not a int", ex);
        }
    }

    public static int getInteger(IProperties prop, String key) throws IgnisException {
        return (int) getLong(prop, key);
    }

}
