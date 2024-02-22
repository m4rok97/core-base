package org.ignis.submitter;


import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.watch.WatchEvent;
import io.grpc.netty.GrpcSslContexts;
import org.ignis.properties.ICrypto;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.properties.IPropertyException;
import org.ignis.scheduler3.ISchedulerException;
import org.ignis.scheduler3.ISchedulerParser;
import org.ignis.scheduler3.ISchedulerUtils;
import org.ignis.scheduler3.model.IClusterRequest;
import org.ignis.scheduler3.model.IContainerInfo;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@CommandLine.Command(name = "run", description = "Run a job")
public class RunJob extends BaseJob {
    @CommandLine.Spec
    CommandSpec spec;

    @Parameters(index = "0", paramLabel = "command", description = "command to run", arity = "1",
            preprocessor = AllArgs.class)
    private String cmd;

    @Parameters(index = "1", paramLabel = "args", arity = "*", description = "arguments for the command")
    private List<String> args;

    @Option(names = {"-n", "--name"}, paramLabel = "str", description = "specify a name for the job")
    private String name;

    @Option(names = {"-p", "--property"}, paramLabel = "<key=value>", description = "set a jot property")
    private void setPropertyArg(Map<String, String> p) {
        this.propertyArg = p;
    }

    @Option(names = {"-f", "--property-file"}, paramLabel = "FILE", description = "Job properties file")
    void setPropertyFile(String f) {
        this.propertyFile = f;
    }

    @Option(names = {"-i", "--interactive"}, description = "attach to STDIN, STDOUT and STDERR, but job die when you exit")
    private boolean interactive;

    @Option(names = {"-e", "--env"}, paramLabel = "<key=value>", description = "set a job enviroment variable")
    private Map<String, String> env;

    @Option(names = {"-b", "--bind"}, paramLabel = "<key[=value]>", description = "set a job bind path",
            preprocessor = NoKeyValue.class)
    private Map<String, String> bind;

    private String time;

    @Option(names = {"-s", "--static"}, paramLabel = "path",
            description = "force static allocation, cluster properties are load from a file. Use 'int' for a homogeneous cluster")
    private String staticConfig;

    @Option(names = {"-t", "--time"}, paramLabel = "[dd-]hh:mm:ss", description = "set a limit on the total run time of the job")
    private void setTime(String time) {
        if (time.matches("(([0-9]+-)?[1-5]?[0-9]:)?[1-5][0-9]:[1-5][0-9]")) {
            this.time = time;
        }
        throw new CommandLine.ParameterException(spec.commandLine(),
                String.format("Invalid value '%s' for option '-t/--time'", time));
    }

    @Option(names = {"--verbose"}, description = "display detailed information about the job's execution")
    private void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    static class AllArgs implements CommandLine.IParameterPreprocessor {

        @Override
        public boolean preprocess(Stack<String> stack, CommandSpec spec, ArgSpec a, Map<String, Object> info) {
            spec.positionalParameters().get(0).setValue(stack.pop());
            var args = new ArrayList<>();
            while (!stack.empty()) {
                args.add(stack.pop());
            }
            spec.positionalParameters().get(1).setValue(args);
            return true;
        }
    }

    static class NoKeyValue implements CommandLine.IParameterPreprocessor {

        @Override
        public boolean preprocess(Stack<String> stack, CommandSpec s, ArgSpec a, Map<String, Object> info) {
            var bind = stack.pop();
            if (!bind.contains("=")) {
                bind += "=:";
            }
            stack.push(bind);
            return false;
        }
    }

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(RunJob.class);

    private ICrypto.IKeyPair keyPair;

    @Override
    public void run() throws Exception {
        if (name == null) {
            name = cmd;
        }
        if (env != null) {
            for (var entry : env.entrySet()) {
                props.setProperty(IProperties.join(IKeys.DRIVER_ENV, entry.getKey()), entry.getValue());
                props.setProperty(IProperties.join(IKeys.EXECUTOR_ENV, entry.getKey()), entry.getValue());
            }
        }
        if (bind != null) {
            for (var entry : bind.entrySet()) {
                props.setProperty(IProperties.join(IKeys.DRIVER_BINDS, entry.getKey()), entry.getValue());
                props.setProperty(IProperties.join(IKeys.EXECUTOR_BINDS, entry.getKey()), entry.getValue());
            }
        }
        var wd = props.getProperty(IKeys.WDIR);
        props.setProperty(IProperties.join(IKeys.DRIVER_BINDS, wd), wd);
        props.setProperty(IProperties.join(IKeys.EXECUTOR_BINDS, wd), wd);

        var driverArgs = new ArrayList<String>();
        var schedulerParser = new ISchedulerParser(props);
        boolean hostNetwork = schedulerParser.networkMode().equals(IContainerInfo.INetworkMode.HOST);
        boolean isStatic = staticConfig != null;

        if (props.hasProperty(IKeys.CRYPTO_SECRET)) {
            var path = new File(props.getProperty(IKeys.CRYPTO_SECRET));
            if (!path.isFile()) {
                throw new IPropertyException(IKeys.CRYPTO_SECRET, path + " not found error");
            }
            LOGGER.info(IKeys.CRYPTO_SECRET + " set");
        } else {
            LOGGER.info(IKeys.CRYPTO_SECRET + " not set");
        }

        if (interactive || isStatic) {
            keyPair = ICrypto.genKeyPair();
            if (isStatic) {
                props.setProperty(IKeys.CRYPTO_$PRIVATE$, keyPair.privateKey());
            }
            props.setProperty(IKeys.CRYPTO_PUBLIC, keyPair.publicKey());
        }

        if (!hostNetwork) {
            props.setProperty(IProperties.join(IKeys.DRIVER_PORTS, "tcp", props.getString(IKeys.PORT)), "0");
            props.setProperty(IProperties.join(IKeys.DRIVER_PORTS, "tcp", props.getString(IKeys.HEALTHCHECK_PORT)), "0");
            props.setList(IProperties.join(IKeys.DRIVER_PORTS, "tcp", "ignismpi"),
                    Collections.nCopies(props.getInteger(IKeys.TRANSPORT_PORTS), "0"));
        }

        if (interactive) {
            driverArgs.add("ignis-sshserver");
            driverArgs.add("driver");
            if (hostNetwork) {
                driverArgs.add("0");
            } else {
                driverArgs.add(props.getString(IKeys.PORT));
            }
        } else {
            driverArgs.add("ignis-job");
            driverArgs.add(cmd);
            if (args != null) {
                driverArgs.addAll(args);
            }
        }

        if (isStatic) {
            driverArgs.add(0, "ignis-healthcheck");
        }

        var executors = new IClusterRequest[0];
        var options = new ArrayList<String>();
        options.add(props.store64());
        if (isStatic) {
            var execArgs = List.of("ignis-sshserver", "executor",
                    String.valueOf(hostNetwork ? 0 : props.getString(IKeys.PORT)));
            Consumer<IProperties> setPorts = (p) -> {
                if (!hostNetwork) {
                    props.setProperty(IProperties.join(IKeys.EXECUTOR_PORTS, "tcp", props.getString(IKeys.PORT)), "0");
                    props.setList(IProperties.join(IKeys.EXECUTOR_PORTS, "tcp", "ignismpi"),
                            Collections.nCopies(props.getInteger(IKeys.TRANSPORT_PORTS), "0"));
                }
            };

            if (staticConfig.matches("\\d+")) {
                executors = new IClusterRequest[Integer.parseInt(staticConfig)];
                setPorts.accept(props);
                for (int i = 0; i < executors.length; i++) {
                    var name = (i + 1) + "-" + props.getProperty(IKeys.EXECUTOR_NAME);
                    executors[i] = schedulerParser.parse(IKeys.EXECUTOR, name, execArgs);
                }
            } else {
                var multiple = props.multiLoad(staticConfig);
                executors = new IClusterRequest[multiple.size()];
                for (int i = 0; i < executors.length; i++) {
                    var name = (i + 1) + "-" + props.getProperty(IKeys.EXECUTOR_NAME);
                    setPorts.accept(multiple.get(i));
                    options.add(multiple.get(i).store64());
                    var parser = new ISchedulerParser(multiple.get(i));
                    executors[i] = parser.parse(IKeys.EXECUTOR, name, execArgs);
                }
            }
        }

        props.setProperty(IProperties.join(IKeys.DRIVER_ENV, IProperties.asEnv(IKeys.OPTIONS)),
                String.join(";", options));
        var driverName = "0-" + props.getProperty(IKeys.DRIVER_NAME);
        var driver = schedulerParser.parse(IKeys.DRIVER, driverName, driverArgs);

        if (Boolean.getBoolean(IKeys.DEBUG)) {
            LOGGER.info(props.toString());
            LOGGER.info("Driver Request: {" + ISchedulerUtils.yaml(driver) + "\n}\n");
            for (var e : executors) {
                LOGGER.info("Executor Request: {" + ISchedulerUtils.yaml(e) + "\n}\n");
            }
        }

        String jobID = scheduler.createJob(name, driver, executors);
        if (interactive) {
            System.exit(connect(jobID, driverName));
        } else {
            System.out.println("submitted job " + jobID);
        }
    }

    private synchronized int connect(String jobID, String driverName) throws Exception {
        var cancel = new Thread(() -> {
            try {
                scheduler.cancelJob(jobID);
            } catch (ISchedulerException ex) {
                LOGGER.error(ex.getMessage(), ex);
            }
        });
        Runtime.getRuntime().addShutdownHook(cancel);
        var job = scheduler.getJob(jobID);
        IContainerInfo driver = null;
        for (var cluster : job.clusters()) {
            if (cluster.id().equals(driverName)) {
                driver = cluster.containers().get(0);
                break;
            }
        }

        if (driver == null) {
            LOGGER.error("Driver not found");
            System.exit(-1);
        }

        while (scheduler.getContainerStatus(jobID, driver.id()).equals(IContainerInfo.IStatus.ACCEPTED)) {
            Thread.sleep(5000);
        }

        var user = driver.user().split(":")[0];
        var port = driver.network().equals(IContainerInfo.INetworkMode.HOST) ? discovery(jobID, driver.id()) :
                driver.hostPort(props.getInteger(IKeys.PORT));

        var jsch = new JSch();
        var session = jsch.getSession(user, driver.node(), port);
        session.setConfig("StrictHostKeyChecking", "no");
        jsch.addIdentity(user, keyPair.privateKey().getBytes(), keyPair.publicKey().getBytes(), null);

        for (int i = 0; i < 60; i++) {
            try {
                session.connect(60000);
                break;
            } catch (JSchException ex) {
                if (!(ex.getCause() != null && ex.getCause() instanceof IOException) || i == 9) {
                    throw ex;
                }
                Thread.sleep(1000);
            }
        }
        var channel = (ChannelExec) session.openChannel("exec");
        channel.setOutputStream(System.out, true);
        channel.setErrStream(System.err, true);
        channel.setInputStream(System.in, true);

        var driverArgs = new ArrayList<String>();
        driverArgs.add("ignis-client");
        driverArgs.add("ignis-job");
        driverArgs.add(cmd);
        if (args != null) {
            driverArgs.addAll(args);
        }
        channel.setCommand(
                driverArgs.stream().map((a) -> '"' + a.replace("\"", "\\\"") + '"').
                        collect(Collectors.joining(" "))
        );
        channel.connect();
        while (!channel.isClosed()) {
            Thread.sleep(5000);
        }
        channel.disconnect();
        session.disconnect();
        Runtime.getRuntime().removeShutdownHook(cancel);
        return channel.getExitStatus();
    }

    private synchronized Integer discovery(String jobID, String driverId) throws Exception {
        AtomicInteger port = new AtomicInteger(-1);
        if (props.getProperty(IKeys.DISCOVERY_TYPE).equals("file")) {
            fileDiscovery(jobID, driverId, port);
        } else {
            etcdDiscovery(jobID, driverId, port);
        }
        this.wait();
        if (port.get() == -1) {
            throw new RuntimeException("port discovery error");
        }
        return port.get();
    }

    private void etcdDiscovery(String jobID, String driverId, AtomicInteger port) {
        try {
            var endpoint = props.getProperty(IKeys.DISCOVERY_ETCD_ENDPOINT);
            var ca = props.getString(IKeys.DISCOVERY_ETCD_CA, null);
            var builder = Client.builder().waitForReady(false).endpoints(endpoint);
            var user = props.getProperty(IKeys.DISCOVERY_ETCD_USER, null);
            var password = props.getProperty(IKeys.DISCOVERY_ETCD_$PASSWORD$, null);
            if (ca == null) {
                builder.sslContext(GrpcSslContexts.forClient().trustManager(ICrypto.selfSignedTrust()).build());
            } else {
                builder.sslContext(GrpcSslContexts.forClient().trustManager(new File(ca)).build());
            }
            if (user != null) {
                builder.user(ByteSequence.from(user.getBytes()));
            }
            if (password != null) {
                builder.password(ByteSequence.from(password.getBytes()));
            }
            Thread.ofPlatform().name("discovery").daemon().start(() -> {
                try (var client = builder.build()) {
                    var parent = this;
                    var key = ByteSequence.from((jobID + "/" + driverId + "-discovery").getBytes());
                    synchronized (this) {
                        var watch = client.getWatchClient().watch(key,
                                watchResponse -> {
                                    for (var event : watchResponse.getEvents()) {
                                        if (event.getEventType() == WatchEvent.EventType.PUT) {
                                            try {
                                                port.set(Integer.parseInt(event.getKeyValue().getValue().toString()));
                                            } catch (Exception ex) {
                                                LOGGER.error(ex.getMessage(), ex);
                                            } finally {
                                                synchronized (parent) {
                                                    parent.notifyAll();
                                                }
                                            }
                                            break;
                                        }
                                    }
                                }
                        );
                        try {
                            var values = client.getKVClient().get(key).get().getKvs();
                            if (!values.isEmpty()) {
                                try {
                                    port.set(Integer.parseInt(values.get(0).getValue().toString()));
                                } finally {
                                    parent.notifyAll();
                                }
                            } else {
                                this.wait();
                            }
                        } catch (Exception ex) {
                            LOGGER.error(ex.getMessage(), ex);
                        }
                        watch.close();
                    }
                }
            });
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    private void fileDiscovery(String jobID, String driverId, AtomicInteger port) {
        var file = Paths.get(props.getProperty(IKeys.WDIR), jobID, "tmp", driverId, "discovery").toFile();
        var delete = new Thread(() -> file.delete());
        Runtime.getRuntime().addShutdownHook(delete);
        Thread.ofPlatform().name("discovery").daemon().start(() -> {
            while (true) {
                if (file.exists()) {
                    try {
                        var value = Files.readString(file.toPath());
                        if (value.endsWith("\n")) {
                            port.set(Integer.parseInt(value));
                            file.delete();
                            Runtime.getRuntime().removeShutdownHook(delete);
                            break;
                        }
                    } catch (IOException | NumberFormatException ex) {
                        LOGGER.error(ex.getMessage(), ex);
                    } finally {
                        synchronized (this) {
                            this.notifyAll();
                        }
                    }
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
    }

}
