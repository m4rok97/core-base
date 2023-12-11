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

import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author César Pomar
 */
public final class IProperties {

    private final static Pattern BOOLEAN = Pattern.compile("y|Y|yes|Yes|YES|true|True|TRUE|on|On|ON");
    private final Map<String, String> inner;
    private final IProperties defaults;

    public static String join(String... skeys) {
        return String.join(".", skeys);
    }

    public static String[] split(String key) {
        return key.split("\\.");
    }

    public static String asEnv(String key) {
        return key.toUpperCase().replace(".", "_");
    }

    public void encrypt(String key) {
        try {
            setProperty(key, ICrypto.openssl(getProperty(key), getProperty(IKeys.CRYPTO_SECRET), false));
        } catch (IOException ex) {
            throw new IPropertyException(key, ex.getMessage());
        }
    }

    public void decrypt(String key) {
        try {
            setProperty(key, ICrypto.openssl(getProperty(key), getProperty(IKeys.CRYPTO_SECRET), true));
        } catch (IOException ex) {
            throw new IPropertyException(key, ex.getMessage());
        }
    }

    public IProperties(IProperties defaults) {
        this.defaults = defaults;
        inner = new HashMap<>();
    }

    public IProperties() {
        this(null);
    }


    public IProperties copy() {
        IProperties copy = new IProperties(defaults);
        copy.inner.putAll(inner);
        return copy;
    }

    private String nn(String value) {
        if (value == null) {
            return "";
        }
        return value;
    }

    public String setProperty(String key, String value) {
        return nn(inner.put(nn(key), nn(value)));
    }

    public String getProperty(String key) throws IPropertyException {
        var value = inner.get(nn(key));
        if (value != null) {
            return value;
        } else if (defaults != null) {
            return defaults.getProperty(key);
        }
        throw new IPropertyException(nn(key), "value not found");
    }

    public String getProperty(String key, String def) {
        var value = inner.get(nn(key));
        if (value != null) {
            return value;
        } else if (defaults != null) {
            return defaults.getProperty(key, def);
        }
        return def;
    }

    public boolean hasProperty(String key) {
        return inner.containsKey(nn(key)) || (defaults != null && defaults.hasProperty(key));
    }

    public String rmProperty(String key) {
        return nn(inner.remove(nn(key)));
    }

    private <T> List<T> getList(String key, Function<String, T> f) {
        try {
            return Arrays.stream(getProperty(nn(key)).split(",")).map(f).collect(Collectors.toList());
        } catch (NumberFormatException ex) {
            throw new IPropertyException(nn(key), ex.toString());
        }
    }

    private static boolean isTrue(String value) {
        return BOOLEAN.matcher(value).matches();
    }

    public boolean getBoolean(String key) throws IPropertyException {
        return isTrue(getProperty(key));
    }

    public boolean getBoolean(String key, boolean def) throws IPropertyException {
        if (hasProperty(key)) {
            return getBoolean(key);
        }
        return def;
    }

    public List<Boolean> getBooleanList(String key) throws IPropertyException {
        return getList(key, IProperties::isTrue);
    }

    public int getInteger(String key) throws IPropertyException {
        try {
            return Integer.parseInt(getProperty(key));
        } catch (NumberFormatException ex) {
            throw new IPropertyException(nn(key), ex.toString());
        }
    }

    public int getInteger(String key, int def) throws IPropertyException {
        if (hasProperty(key)) {
            return getInteger(key);
        }
        return def;
    }

    public List<Integer> getIntegerList(String key) throws IPropertyException {
        return getList(key, Integer::parseInt);
    }

    public long getLong(String key) throws IPropertyException {
        try {
            return Long.parseLong(getProperty(key));
        } catch (NumberFormatException ex) {
            throw new IPropertyException(nn(key), ex.toString());
        }
    }

    public long getLong(String key, long def) throws IPropertyException {
        if (hasProperty(key)) {
            return getLong(key);
        }
        return def;
    }

    public List<Long> getLongList(String key) throws IPropertyException {
        return getList(key, Long::parseLong);
    }

    public float getFloat(String key) throws IPropertyException {
        try {
            return Float.parseFloat(getProperty(key));
        } catch (NumberFormatException ex) {
            throw new IPropertyException(nn(key), ex.toString());
        }
    }

    public float getFloat(String key, float def) throws IPropertyException {
        if (hasProperty(key)) {
            return getFloat(key);
        }
        return def;
    }


    public List<Float> getFloatList(String key) throws IPropertyException {
        return getList(key, Float::parseFloat);
    }

    public double getDouble(String key) throws IPropertyException {
        try {
            return Double.parseDouble(getProperty(key));
        } catch (NumberFormatException ex) {
            throw new IPropertyException(nn(key), ex.toString());
        }
    }

    public double getDouble(String key, double def) throws IPropertyException {
        if (hasProperty(key)) {
            return getDouble(key);
        }
        return def;
    }

    public List<Double> getDoubleList(String key) throws IPropertyException {
        return getList(key, Double::parseDouble);
    }

    public String getString(String key) throws IPropertyException {
        return getProperty(key);
    }

    public List<String> getStringList(String key) throws IPropertyException {
        return getList(key, (s) -> s);
    }

    public IProperties withPrefix(String key) {
        var pp = new IProperties();
        pp.inner.putAll(toMap(true));
        pp.inner.entrySet().removeIf((e) -> !e.getKey().startsWith(key + "."));
        return pp;
    }

    public Map<String, String> toMap(boolean def) {
        var result = new HashMap<String, String>();
        if (def && defaults != null) {
            result.putAll(defaults.toMap(def));
        }
        result.putAll(inner);
        return result;
    }

    public void fromMap(Map<String, String> map) {
        inner.putAll(map);
    }

    public List<IProperties> multiLoad(String path) throws IOException {
        var result = new ArrayList<IProperties>();
        var yamlMapper = new YAMLMapper();
        var javaMapper = new JavaPropsMapper();
        var parser = yamlMapper.createParser(path);
        var dataList = yamlMapper.readValues(parser, HashMap.class).readAll();
        for (var data : dataList) {
            var tmp = javaMapper.writeValueAsMap(data);
            var copy = this.copy();
            copy.inner.putAll(tmp);
            result.add(copy);
        }
        return result;
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
        var yamlMapper = new YAMLMapper();
        var javaMapper = new JavaPropsMapper();
        var data = yamlMapper.readValue(in, HashMap.class);
        var tmp = javaMapper.writeValueAsMap(data);
        for (var entry : tmp.entrySet()) {
            if (replace) {
                inner.put(entry.getKey(), entry.getValue());
            } else {
                inner.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
    }

    public void load64(String s) throws IOException {
        load64(s, true);
    }

    public void load64(String s, boolean replace) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(Base64.getDecoder().decode(s));
        load(bis, replace);
    }

    public void store(String path) throws IOException {
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(path))) {
            store(out);
        }
    }

    public void store(OutputStream out) throws IOException {
        var yamlMapper = new YAMLMapper();
        var javaMapper = new JavaPropsMapper();
        var data = javaMapper.readMapAs(this.toMap(true), HashMap.class);
        yamlMapper.writeValue(out, data);
    }

    public String store64() throws IOException {
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
        return "IProperties{\n" + writer + '}';
    }


}
