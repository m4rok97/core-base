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
public class IPropertyParser {

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

    public static long getSILong(IProperties prop, String key) throws IgnisException {
        String str = getString(prop, key).trim();
        final String UNITS = "KMGTPEZY";
        double num;
        int base;
        int exp = 0;
        boolean decimal = false;
        int i = 0;
        int len = str.length();
        char[] cs = str.toCharArray();
        while (i < len && cs[i] == ' ') {
        }
        while (i < len) {
            if (cs[i] >= '0' && cs[i] <= '9') {
                i++;
            } else if (!decimal && (cs[i] == '.' || cs[i] == ',')) {
                i++;
                decimal = true;
            } else {
                break;
            }
        }
        num = Double.parseDouble(str.substring(0, i));
        if (i < len) {
            if (cs[i] == ' ') {
                i++;
            }
        }
        if (i < len) {
            exp = UNITS.indexOf(Character.toUpperCase(cs[i])) + 1;
            if (exp > 0) {
                i++;
            }
        }
        if (i < len && exp > 0 && cs[i] == 'i') {
            i++;
            base = 1024;
        } else {
            base = 1000;
        }
        if (i < len) {
            switch (cs[i++]) {
                case 'B':
                    //Nothing
                    break;
                case 'b':
                    num = num / 8;
                    break;
                default:
                    throw new IgnisException(key + " has an invalid value");
            }
        }
        if (i != len) {
            throw new IgnisException(key + " has an invalid value");
        }
        return (long) Math.ceil(num * Math.pow(base, exp));
    }

}
