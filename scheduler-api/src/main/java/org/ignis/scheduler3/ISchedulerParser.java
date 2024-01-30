package org.ignis.scheduler3;

import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.properties.IPropertyException;
import org.ignis.scheduler3.model.*;
import com.sun.security.auth.module.UnixSystem;

import java.io.File;
import java.util.*;

public final class ISchedulerParser {

    private final IProperties props;

    public ISchedulerParser(IProperties props) {
        this.props = props;
    }

    public IProperties getProperties() {
        return props;
    }

    public IClusterRequest parse(String prefix, String name, List<String> args) {
        var builder = create();
        builder.args(args);
        var ports = new ArrayList<IPortMapping>();
        builder.ports(ports);
        var binds = new ArrayList<IBindMount>();
        builder.binds(binds);
        var hostnames = new HashMap<String, String>();
        builder.hostnames(hostnames);
        var env = new HashMap<String, String>();
        var setenv = new HashMap<String, Boolean>();
        builder.env(env);
        var schedulerOptArgs = new HashMap<String, String>();
        builder.schedulerOptArgs(schedulerOptArgs);
        String image = null;
        for (var subkey : props.withPrefix(prefix).toMap(true).keySet()) {
            var key = IProperties.join(prefix, subkey);
            var parent = key;
            SEARCH:
            while (!parent.isEmpty()) {
                switch (parent) {
                    case IKeys.DRIVER_IMAGE:
                    case IKeys.EXECUTOR_IMAGE:
                        image = props.getProperty(key);
                        break SEARCH;
                    case IKeys.DRIVER_CORES:
                    case IKeys.EXECUTOR_CORES:
                        builder.cpus(props.getInteger(key));
                        break SEARCH;
                    case IKeys.DRIVER_GPU:
                    case IKeys.EXECUTOR_GPU:
                        builder.gpu(props.getProperty(key));
                        break SEARCH;
                    case IKeys.DRIVER_MEMORY:
                    case IKeys.EXECUTOR_MEMORY:
                        builder.memory(props.getSILong(key));
                        break SEARCH;
                    case IKeys.DRIVER_PORTS:
                    case IKeys.EXECUTOR_PORTS:
                        ports.addAll(parsePort(prefix, key, props.getProperty(key)));
                        break SEARCH;
                    case IKeys.DRIVER_BINDS:
                    case IKeys.EXECUTOR_BINDS:
                        binds.add(parseBind(prefix, key, props.getProperty(key)));
                        break SEARCH;
                    case IKeys.DRIVER_NODELIST:
                    case IKeys.EXECUTOR_NODELIST:
                        builder.nodelist(props.getStringList(key));
                        break SEARCH;
                    case IKeys.DRIVER_ENV:
                    case IKeys.EXECUTOR_ENV:
                        parseKeyValue(prefix, env, key, props.getProperty(key));
                        break SEARCH;
                    case IKeys.DRIVER_SETENV:
                    case IKeys.EXECUTOR_SETENV:
                        if (key.endsWith("..")) {//bypass .$crypto$= keys
                            key = key.substring(0, key.length() - 2);
                        }
                        setenv.put(key, props.getBoolean(key));
                        break SEARCH;
                }
                parent = IProperties.parent(parent);
            }
        }
        containerEnvironment(setenv, env);
        builder.image(parseImage(image));
        if (IKeys.EXECUTOR_INSTANCES.startsWith(prefix)) {
            return new IClusterRequest(name, builder.build(), props.getInteger(IKeys.EXECUTOR_INSTANCES));
        }
        return new IClusterRequest(name, builder.build(), 1);
    }

    public IContainerInfo.INetworkMode networkMode() {
        String network;
        if (props.getString(IKeys.CONTAINER_PROVIDER).equals("docker")) {
            network = props.getString(IKeys.CONTAINER_DOCKER_NETWORK).toUpperCase();
            if (network.equals("DEFAULT")) {
                network = "BRIDGE";
            }
        } else {
            network = props.getString(IKeys.CONTAINER_SINGULARITY_NETWORK).toUpperCase();
            if (network.equals("DEFAULT")) {
                network = "HOST";
            }
        }
        return IContainerInfo.INetworkMode.valueOf(network);
    }

    private IContainerInfo.IContainerInfoBuilder create() {
        var builder = IContainerInfo.builder();
        builder.time(parseTime());
        var provider = props.getString(IKeys.CONTAINER_PROVIDER).equals("docker") ? IContainerInfo.IProvider.DOCKER :
                IContainerInfo.IProvider.SINGULARITY;
        if (provider.equals(IContainerInfo.IProvider.DOCKER) && props.getBoolean(IKeys.CONTAINER_DOCKER_ROOT)) {
            builder.user("root:0:0");
        } else {
            UnixSystem user = new UnixSystem();
            builder.user(user.getUsername() + ":" + user.getUid() + ":" + user.getGid());
        }
        builder.provider(provider);
        builder.network(networkMode());
        builder.writable(props.getBoolean(IKeys.CONTAINER_WRITABLE));
        if (props.hasProperty(IKeys.TMPDIR)) {
            builder.tmpdir(props.getString(IKeys.TMPDIR));
        }
        return builder;
    }


    private String parseImage(String image) {
        String prefix = "";
        if (props.getString(IKeys.CONTAINER_PROVIDER).equals("singularity")) {
            if (image == null) {
                var src = props.getString(IKeys.CONTAINER_SINGULARITY_SOURCE);
                image = src + (src.endsWith("/") ? "" : "/") + props.getString(IKeys.CONTAINER_SINGULARITY_DEFAULT);
            } else if (image.contains(":") || (!image.isEmpty() && image.charAt(0) == '/')) {
                var src = props.getString(IKeys.CONTAINER_SINGULARITY_SOURCE);
                image = src + (src.endsWith("/") ? "" : "/") + image;
            }

            if (new File(image).exists() || image.contains(":")) {
                return image;
            }
            prefix = "docker://";
        }
        if (image == null) {
            image = props.getString(IKeys.CONTAINER_DOCKER_DEFAULT);
        }
        if (!image.contains("/")) {
            var n = props.getString(IKeys.CONTAINER_DOCKER_NAMESPACE);
            if (!n.isEmpty() && n.charAt(n.length() - 1) != '/') {
                n += "/";
            }
            image = n + image;
        }

        if (image.indexOf('/') != image.lastIndexOf('/')) {
            var r = props.getString(IKeys.CONTAINER_DOCKER_REGISTRY);
            if (!r.isEmpty() && r.charAt(r.length() - 1) != '/') {
                r += "/";
            }
            image = r + image;
        }

        if (!image.contains(":") || image.indexOf(':') > image.lastIndexOf('/')) {
            var t = props.getString(IKeys.CONTAINER_DOCKER_TAG);
            if (!t.startsWith(":")) {
                t = ":" + t;
            }
            image += t;
        }
        return prefix + image;
    }

    private IBindMount parseBind(String prefix, String key, String value) {
        var keys = IProperties.split(key);
        var container = IProperties.relative(prefix, key).substring("binds.".length());
        if (value.contains(":")) {
            var values = value.split(":");
            if (values.length != 2) {
                throw new IPropertyException(key, "has bad format");
            }
            if (values[0].isEmpty()) {
                value = container;
            }
            return new IBindMount(container, value, values[1].equals("ro"));
        }
        return new IBindMount(container, value, false);
    }

    private void parseKeyValue(String prefix, Map<String, String> dest, String key, String value) {
        var keys = IProperties.split(IProperties.relative(prefix, key));
        if (keys.length != 2) {
            throw new IPropertyException(key, "has bad format");
        }
        dest.put(keys[1], value);
    }

    private Long parseTime() {
        if (props.hasProperty(IKeys.TIME)) {
            var text = props.getString(IKeys.TIME);
            var fields = text.split("[-:]");
            Collections.reverse(Arrays.asList(fields));
            int[] w = {1, 60, 60 * 60, 60 * 60 * 24};
            long seconds = 0;
            for (int i = 0; i < fields.length; i++) {
                try {
                    seconds += Long.parseLong(fields[i]) * w[i];
                } catch (NumberFormatException ex) {
                    throw new IPropertyException(IKeys.TIME, "has bad format");
                }
            }
            return seconds;
        }
        return null;
    }

    private List<IPortMapping> parsePort(String prefix, String key, String value) {
        var ports = new ArrayList<IPortMapping>();
        var keys = IProperties.split(IProperties.relative(prefix, key));
        if (keys.length != 3) {
            throw new IPropertyException(key, "has bad format, use " +
                    "*.ports.{type}.{container_port}={host_port} or *.ports.{type}.{name}=0,...");
        }
        if (keys[1].startsWith("tcp") || keys[1].startsWith("udp")) {
            var proto = IPortMapping.Protocol.valueOf(keys[1].toUpperCase());
            if (keys[2].matches("-?\\d+")) {
                ports.add(new IPortMapping(Integer.parseInt(keys[2]), Integer.parseInt(value), proto));
                if (ports.getLast().container() < 1) {
                    throw new IPropertyException(key, "container port must be >0, found " + keys[2]);
                }
                if (ports.getLast().host() < 0) {
                    throw new IPropertyException(key, "host port must be >=0, found " + value);
                }
            } else {
                for (var port : value.split(",")) {
                    ports.add(new IPortMapping(0, Integer.parseInt(port), proto));
                    if (ports.getLast().host() != 0) {
                        throw new IPropertyException(key, "random ports must be 0, found " + port);
                    }
                }
            }
        } else {
            throw new IPropertyException(key, "expected type tcp/udp, found " + keys[1]);
        }
        return ports;
    }

    public Map<String, String> dumpPorts(String prefix, IContainerInfo info) {
        var result = new IProperties();
        if (info.network().equals(IContainerInfo.INetworkMode.HOST)) {
            return Map.of();
        }
        for (var proto : List.of("tcp", "udp")) {
            var free = new LinkedList<String>();
            for (var port : info.ports()) {
                if (!port.protocol().name().equalsIgnoreCase(proto)) {
                    continue;
                }
                var n = IProperties.join(prefix, proto, String.valueOf(port.container()));
                if (props.hasProperty(n)) {
                    result.setProperty(n, port.host());
                } else {
                    free.add(String.valueOf(port.host()));
                }
            }

            List<String> named = props.withPrefix(IProperties.join(prefix, proto)).toMap(true).keySet().stream().
                    filter((s) -> !s.matches("-?\\d+")).sorted().toList();

            for (var id : named) {
                var portName = IProperties.join(prefix, proto, id);
                int n = props.getStringList(portName).size();
                if (n > free.size()) {
                    throw new IPropertyException(portName, "scheduler assignment not enough ports");
                }
                result.setList(portName, free.subList(free.size() - n, free.size()));
                for (int i = 0; i < n; i++) {
                    free.removeLast();
                }
            }
        }

        return result.toMap(true);
    }

    public void containerEnvironment(Map<String, Boolean> setenv, Map<String, String> env) {
        props.toEnv(IKeys.WDIR, env, true);
        props.toEnv(IKeys.TRANSPORT_COMPRESSION, env, true);

        props.toEnv(IKeys.DEBUG, env, false);
        props.toEnv(IKeys.VERBOSE, env, false);
        props.toEnv(IKeys.TMPDIR, env, false);
        props.toEnv(IKeys.HEALTHCHECK_INTERVAL, env, true);
        props.toEnv(IKeys.HEALTHCHECK_TIMEOUT, env, true);
        props.toEnv(IKeys.HEALTHCHECK_RETRIES, env, true);
        props.toEnv(IKeys.HEALTHCHECK_URL, env, false);

        props.toEnv(IKeys.CRYPTO_PUBLIC, env, false);
        props.toEnv(IKeys.CRYPTO_SECRET, env, false);
        props.toEnv(IKeys.DISCOVERY_TYPE, env, false);
        props.toEnv(IKeys.DISCOVERY_ETCD_USER, env, false);
        props.toEnv(IKeys.DISCOVERY_ETCD_$PASSWORD$, env, false);
        props.toEnv(IKeys.DISCOVERY_ETCD_ENDPOINT, env, false);
        props.toEnv(IKeys.DISCOVERY_ETCD_CA, env, false);

        for (var entry : setenv.entrySet()) {
            props.toEnv(entry.getKey(), env, entry.getValue());
        }
    }

}
