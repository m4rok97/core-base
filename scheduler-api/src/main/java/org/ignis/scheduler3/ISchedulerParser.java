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


    public IClusterRequest parse(String prefix, List<String> args) {
        var builder = create();
        builder.args(args);
        var ports = new ArrayList<IPortMapping>();
        builder.ports(ports);
        var binds = new ArrayList<IBindMount>();
        builder.binds(binds);
        var hostnames = new HashMap<String, String>();
        builder.hostnames(hostnames);
        var env = new HashMap<String, String>();
        builder.env(env);
        var schedulerOptArgs = new HashMap<String, String>();
        builder.schedulerOptArgs(schedulerOptArgs);
        String name = null;
        String image = null;
        for (var subkey : props.withPrefix(prefix).toMap(true).keySet()) {
            var key = IProperties.join(prefix, subkey);
            var parent = key;
            SEARCH:
            while (!parent.isEmpty()) {
                switch (parent) {
                    case IKeys.DRIVER_NAME:
                    case IKeys.EXECUTOR_NAME:
                        name = props.getProperty(key);
                        break SEARCH;
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
                    case IKeys.DRIVER_HOSTNAMES:
                    case IKeys.EXECUTOR_HOSTNAMES:
                        parseKeyValue(prefix, hostnames, key, props.getProperty(key));
                        break SEARCH;
                    case IKeys.DRIVER_ENV:
                    case IKeys.EXECUTOR_ENV:
                        parseKeyValue(prefix, env, key, props.getProperty(key));
                        break SEARCH;
                }
                parent = IProperties.parent(parent);
            }
        }

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
        UnixSystem user = new UnixSystem();
        builder.user(user.getUsername() + ":" + user.getUid() + ":" + user.getGid());
        var provider = props.getString(IKeys.CONTAINER_PROVIDER).equals("docker") ? IContainerInfo.IProvider.DOCKER :
                IContainerInfo.IProvider.SINGULARITY;
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
                throw new IPropertyException(key, " has bad format");
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
            throw new IPropertyException(key, " has bad format");
        }
        dest.put(keys[1], value);
    }

    public Long parseTime() {
        if (props.hasProperty(IKeys.JOB_TIME)) {
            var text = props.getString(IKeys.JOB_TIME);
            var fields = text.split("[-:]");
            Collections.reverse(Arrays.asList(fields));
            int[] w = {1, 60, 60 * 60, 60 * 60 * 24};
            long seconds = 0;
            for (int i = 0; i < fields.length; i++) {
                try {
                    seconds += Long.parseLong(fields[i]) * w[i];
                } catch (NumberFormatException ex) {
                    throw new IPropertyException(IKeys.JOB_TIME, " has bad format");
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
            throw new IPropertyException(key, " has bad format, use " +
                    "*.ports.{type}.{container_port}={host_port} or *.ports.{type}.host={host_port},...");
        }
        if (keys[1].startsWith("tcp") || keys[1].startsWith("udp")) {
            var proto = IPortMapping.Protocol.valueOf(keys[1].toUpperCase());
            if (keys[2].equals("host")) {
                for (var port : value.split(",")) {
                    ports.add(new IPortMapping(Integer.parseInt(port), Integer.parseInt(port), proto));
                }
            } else {
                ports.add(new IPortMapping(Integer.parseInt(keys[2]), Integer.parseInt(value), proto));
            }
        } else {
            throw new IPropertyException(key, "expected type tcp/udp, found " + keys[1]);
        }
        return ports;
    }

    public void containerEnvironment(IClusterRequest request, boolean driver, boolean interactive, boolean _static) {
        var env = request.resources().env();
        var hostMode = request.resources().network().equals(IContainerInfo.INetworkMode.HOST);
        props.toEnv(IKeys.WORKING_DIRECTORY, env, true);
        props.toEnv(IKeys.DRIVER_HEALTHCHECK_INTERVAL, env, false);
        props.toEnv(IKeys.DRIVER_HEALTHCHECK_TIMEOUT, env, false);
        props.toEnv(IKeys.DRIVER_HEALTHCHECK_RETRIES, env, false);
        props.toEnv(IKeys.DRIVER_HEALTHCHECK_URL, env, false);

        props.toEnv(IKeys.CRYPTO_SECRET, env, false);
        props.toEnv(IKeys.DISCOVERY_TYPE, env, false);
        props.toEnv(IKeys.DISCOVERY_TARGET, env, false);
        props.toEnv(IKeys.DISCOVERY_ETCD_USER, env, false);
        props.toEnv(IKeys.DISCOVERY_ETCD_PASSWORD, env, false);
        props.toEnv(IKeys.DISCOVERY_ETCD_ENDPOINT, env, false);
        props.toEnv(IKeys.DISCOVERY_ETCD_CA, env, false);

        //TODo TMP DIR ???

    }

}
