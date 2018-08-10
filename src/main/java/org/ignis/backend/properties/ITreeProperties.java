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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 *
 * @author César Pomar
 */
public class ITreeProperties {

    /**
     *
     */
    @JsonDeserialize(using = EntryDeserializer.class)
    @JsonSerialize(using = EntrySerializer.class)
    public final static class Entry {

        private String value;
        private Map<String, Entry> node;

        private Entry() {
            node = new HashMap<>();
        }

        private Entry(String value) {
            this();
            setValue(value);
        }

        private Entry(Map<String, Entry> node) {
            this.node = node;
            Entry e = node.get("value");
            if (e != null) {
                node.putAll(e.node);
                setValue(e.value);
            }
        }

        private Map<String, Entry> getNode() {
            return node;
        }

        private String setValue(String value) {
            if (this.value == null && value != null) {
                node.put("value", this);
            } else if (this.value != null && value == null) {
                node.remove("value");
            }
            String old = this.value;
            this.value = value;
            return old;
        }

        public Set<String> childs() {
            return Collections.unmodifiableSet(node.keySet());
        }

        public Collection<Entry> childsEntry() {
            return Collections.unmodifiableCollection(node.values());
        }

        public Entry getChild(String key) {
            if (key.equals("value")) {
                return this;
            }
            return node.get(key);
        }

        public String getValue() {
            return value;
        }

        public boolean hasValue() {
            return value != null;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(1000);
            sb.append('{');
            if (value != null) {
                sb.append(value);
                sb.append(", ");
            }

            if (!node.isEmpty()) {
                node.entrySet().stream().filter(e -> !e.getKey().equals("value")).forEach(e -> {
                    sb.append(e.getKey());
                    sb.append(" => ");
                    sb.append(e.getValue());
                    sb.append(", ");
                });
                sb.setLength(sb.length() - 2);
            }
            sb.append('}');
            return sb.toString();
        }
    }

    private final static class EntryDeserializer extends StdDeserializer<Entry> {

        public EntryDeserializer() {
            super(Entry.class);
        }

        @Override
        public Entry deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            JavaType string = TypeFactory.defaultInstance().constructType(String.class);
            if (p.getCurrentToken().isStructStart()) {
                JavaType entry = TypeFactory.defaultInstance().constructType(String.class);
                MapType map = TypeFactory.defaultInstance().constructMapType(HashMap.class, string, entry);
                return new Entry(p.getCodec().<Map<String, Entry>>readValue(p, map));
            } else {
                return new Entry((String) p.getCodec().readValue(p, string));
            }
        }
    }

    private final static class EntrySerializer extends StdSerializer<Entry> {

        private final Stack<Entry> stack;

        public EntrySerializer() {
            super(Entry.class);
            stack = new Stack<>();
            stack.push(null);
        }

        @Override
        public void serialize(Entry value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            if (stack.peek() != value && !(value.getNode().size() == 1 && value.getValue() != null)) {
                stack.push(value);
                gen.writeObject(value.getNode());
                stack.pop();
            } else {
                gen.writeString(value.getValue());
            }
        }
    }

    private final Entry root;

    ITreeProperties() {
        root = new Entry();
    }

    private Entry get(String key, boolean create) {
        Entry actual = root;
        for (String subKey : key.split("\\.")) {
            Entry next = actual.getChild(subKey);
            if (next == null) {
                if (create) {
                    next = new Entry();
                    actual.getNode().put(subKey, next);
                } else {
                    return null;
                }
            }
            actual = next;
        }
        return actual;
    }

    private void setAll(Entry rootFrom) {
        Stack<Iterator<Map.Entry<String, Entry>>> stack = new Stack<>();
        Stack<Entry> nodes = new Stack<>();
        nodes.push(root);
        stack.push(rootFrom.getNode().entrySet().iterator());
        while (!stack.isEmpty()) {
            if (!stack.peek().hasNext()) {
                stack.pop();
                nodes.pop();
                continue;
            }
            Map.Entry<String, Entry> from = stack.peek().next();
            if (from.getKey().equals("value")) {
                continue;
            }
            Entry to = nodes.peek().getChild(from.getKey());
            if (to == null) {
                to = new Entry();
                nodes.peek().getNode().put(from.getKey(), to);
            }
            to.setValue(from.getValue().getValue());
            stack.push(from.getValue().getNode().entrySet().iterator());
            nodes.push(to);
        }
    }

    private String remove(String key) {
        Stack<Entry> stack = new Stack<>();
        stack.push(root);
        String[] keys = key.split("\\.");
        String old;
        for (String subKey : keys) {
            stack.push(stack.peek().getChild(subKey));
            if (stack.peek() == null) {
                return null;
            }
        }
        old = stack.peek().getValue();
        stack.peek().setValue(null);
        for (int i = keys.length - 1; i > -1; i--) {
            if (stack.pop().getNode().isEmpty()) {
                stack.peek().getNode().remove(keys[i]);
            } else {
                break;
            }
        }
        return old;
    }

    Entry getRoot() {
        return root;
    }

    Entry getEntry(String key) {
        return get(key, false);
    }

    String setProperty(String key, String value) {
        if (value == null) {
            return remove(key);
        } else {
            Entry e = get(key, true);
            return e.setValue(value);
        }
    }

    void load(File file) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try (Reader r = new FileReader(file); BufferedReader br = new BufferedReader(r)) {
            setAll(mapper.readValue(br, Entry.class));
        }
    }

    void save(File file) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        try (Writer w = new FileWriter(file); BufferedWriter bw = new BufferedWriter(w)) {
            mapper.writeValue(bw, root);
        }
    }

    void toMap(Map<String, String> map) {
        Stack<Iterator<Map.Entry<String, Entry>>> stack = new Stack<>();
        Stack<String> keys = new Stack<>();

        stack.push(root.getNode().entrySet().iterator());
        keys.push("");
        while (!stack.isEmpty()) {
            if (!stack.peek().hasNext()) {
                stack.pop();
                keys.pop();
                continue;
            }
            Map.Entry<String, Entry> entry = stack.peek().next();
            if (entry.getKey().equals("value")) {
                continue;
            }

            stack.push(entry.getValue().getNode().entrySet().iterator());
            keys.push(entry.getKey());

            if (entry.getValue().getValue() != null) {
                map.put(String.join(".", keys).substring(1), entry.getValue().getValue());
            }

        }
    }

    void fromMap(Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            setProperty(entry.getKey(), entry.getValue());
        }
    }

}
