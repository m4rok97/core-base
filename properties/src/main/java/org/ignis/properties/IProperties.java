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
package org.ignis.properties;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author César Pomar
 */
public final class IProperties {

    private final static Pattern BOOLEAN = Pattern.compile("y|Y|yes|Yes|YES|true|True|TRUE|on|On|ON");
    private final Properties inner;
    private final Properties defaults;

    public IProperties(IProperties defaults) {
        this.defaults = defaults.inner;
        inner = new Properties(defaults.inner);
    }

    public IProperties() {
        defaults = null;
        inner = new Properties();
    }

    private IProperties(Properties defaults) {
        this.defaults = defaults;
        inner = new Properties(defaults);
    }


    public IProperties copy() {
        IProperties copy = new IProperties(defaults);
        copy.inner.putAll(inner);
        return copy;
    }

    private String noNull(String value) {
        if (value == null) {
            return "";
        }
        return value;
    }

    public String setProperty(String key, String value) {
        return noNull((String) inner.setProperty(noNull(key), noNull(value)));
    }

    public String getProperty(String key) throws IPropertyException {
        String value = inner.getProperty(noNull(key));
        if (value == null) {
            throw new IPropertyException(noNull(key), " not found");
        }
        return value;
    }

    public String getProperty(String key, String def) {
        String value = inner.getProperty(noNull(key));
        if (value != null) {
            return getProperty(key);
        }
        return def;
    }

    public String rmProperty(String key) {
        return noNull((String) inner.remove(noNull(key)));
    }

    public boolean getBoolean(String key) throws IPropertyException {
        return BOOLEAN.matcher(getProperty(key)).matches();
    }

    public boolean getBoolean(String key, boolean def) throws IPropertyException {
        if (inner.contains(noNull(key))) {
            return getBoolean(key);
        }
        return def;
    }

    public int getInteger(String key) throws IPropertyException {
        try {
            return Integer.parseInt(getProperty(key));
        } catch (NumberFormatException ex) {
            throw new IPropertyException(noNull(key), ex.toString());
        }
    }

    public int getInteger(String key, int def) throws IPropertyException {
        if (inner.contains(noNull(key))) {
            return getInteger(key);
        }
        return def;
    }

    public List<Integer> getIntegerList(String key) throws IPropertyException {
        try {
            return getStringList(key).stream().map((String value) -> Integer.parseInt(value)).collect(Collectors.toList());
        } catch (NumberFormatException ex) {
            throw new IPropertyException(noNull(key), ex.toString());
        }
    }

    public long getLong(String key) throws IPropertyException {
        try {
            return Long.parseLong(getProperty(key));
        } catch (NumberFormatException ex) {
            throw new IPropertyException(noNull(key), ex.toString());
        }
    }

    public long getLong(String key, long def) throws IPropertyException {
        if (inner.contains(noNull(key))) {
            return getLong(key);
        }
        return def;
    }

    public List<Long> getLongList(String key) throws IPropertyException {
        try {
            return getStringList(key).stream().map((String value) -> Long.parseLong(value)).collect(Collectors.toList());
        } catch (NumberFormatException ex) {
            throw new IPropertyException(noNull(key), ex.toString());
        }
    }

    public float getFloat(String key) throws IPropertyException {
        try {
            return Float.parseFloat(getProperty(key));
        } catch (NumberFormatException ex) {
            throw new IPropertyException(noNull(key), ex.toString());
        }
    }

    public float getFloat(String key, float def) throws IPropertyException {
        if (inner.contains(noNull(key))) {
            return getFloat(key);
        }
        return def;
    }

    public List<Float> getFloatList(String key) throws IPropertyException {
        try {
            return getStringList(key).stream().map((String value) -> Float.parseFloat(value)).collect(Collectors.toList());
        } catch (NumberFormatException ex) {
            throw new IPropertyException(noNull(key), ex.toString());
        }
    }

    public double getDouble(String key) throws IPropertyException {
        try {
            return Double.parseDouble(getProperty(key));
        } catch (NumberFormatException ex) {
            throw new IPropertyException(noNull(key), ex.toString());
        }
    }

    public double getDouble(String key, double def) throws IPropertyException {
        if (inner.contains(noNull(key))) {
            return getDouble(key);
        }
        return def;
    }

    public List<Double> getDoubleList(String key) throws IPropertyException {
        try {
            return getStringList(key).stream().map((String value) -> Double.parseDouble(value)).collect(Collectors.toList());
        } catch (NumberFormatException ex) {
            throw new IPropertyException(noNull(key), ex.toString());
        }
    }

    public String getString(String key) throws IPropertyException {
        return getProperty(key);
    }

    public List<String> getStringList(String key) throws IPropertyException {
        return Arrays.asList(getProperty(key).split(","));
    }

    @SuppressWarnings("unchecked")
    public Collection<String> getPrefixKeys(String prefixKey) {
        String prefix = prefixKey + ".";
        return inner.stringPropertyNames().stream().filter((String key) -> key.startsWith(prefix)).collect(Collectors.toList());
    }

    public boolean contains(String key) {
        return inner.getProperty(noNull(key)) != null;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> toMap(boolean useDefaults) {
        if (!useDefaults || this.defaults == null) {
            return new HashMap<>((Map) inner);
        }
        Map<String, String> map = new HashMap<>((Map) this.defaults);
        map.putAll((Map) inner);
        return map;
    }

    public void fromMap(Map<String, String> map) {
        inner.putAll(map);
    }

    public void load(String path) throws IOException {
        load(path, true);
    }

    public void load(String path, boolean replace) throws IOException {
        try (InputStream in = new BufferedInputStream(new FileInputStream(path))) {
            load(in, replace);
        }
    }

    public void load(InputStream in) throws IOException {
        load(in, true);
    }

    public void load(InputStream in, boolean replace) throws IOException {
        if (replace) {
            inner.load(in);
        } else {
            Properties tmp = new Properties();
            tmp.putAll(inner);
            inner.load(in);
            inner.putAll(tmp);
        }
    }

    public void load64(String s) throws IOException{
        load64(s, true);
    }

    public void load64(String s, boolean replace) throws IOException{
        ByteArrayInputStream bis = new ByteArrayInputStream(Base64.getDecoder().decode(s));
        load(bis, replace);
    }

    public void store(String path) throws IOException {
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(path))) {
            store(out);
        }
    }

    public void store(OutputStream out) throws IOException {
        inner.store(out, "Ignis Job properties");
    }

    public String store64() throws IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        store(bos);
        return Base64.getEncoder().encodeToString(bos.toByteArray());
    }

    public void clear() {
        inner.clear();
    }

    public void fromEnv(Map<String, String> env) {
        for (Map.Entry<String, String> entry : env.entrySet()) {
            if (entry.getKey().startsWith("IGNIS_")) {
                setProperty(entry.getKey().replace('_', '.').toLowerCase(), entry.getValue());
            }
        }
    }

    public long getSILong(String key) throws IPropertyException {
        String str = getProperty(key).trim();
        final String UNITS = "KMGTPEZY";
        double num;
        int base;
        int exp = 0;
        boolean decimal = false;
        int i = 0;
        int len = str.length();
        char[] cs = str.toCharArray();
        while (i < len && cs[i] == ' ') {
            i++;
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
                    throw new IPropertyException(key, " has an invalid value");
            }
        }
        if (i != len) {
            throw new IPropertyException(key, " has an invalid value");
        }
        return (long) Math.ceil(num * Math.pow(base, exp));
    }

    @Override
    public String toString() {
        StringBuilder writer = new StringBuilder();
        for (Map.Entry<String, String> entry : toMap(true).entrySet()) {
            writer.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
        }
        return "IProperties{\n" + writer.toString() + '}';
    }


}
