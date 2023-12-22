package org.ignis.submitter;


import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
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
            description = "force static allocation, cluster properties are load from a file. Use '-' for a single cluster")
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

    private volatile Integer interactivePort;

    @Override
    public void run() throws Exception {
        if (name == null) {
            name = cmd;
        }
        props.setProperty(IKeys.JOB_NAME, name);
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
        var wd = props.getProperty(IKeys.WORKING_DIRECTORY);
        props.setProperty(IProperties.join(IKeys.DRIVER_BINDS, wd), wd);
        props.setProperty(IProperties.join(IKeys.EXECUTOR_BINDS, wd), wd);

        var driverArgs = new ArrayList<String>();
        var schedulerParser = new ISchedulerParser(props);
        boolean hostNetwork = schedulerParser.networkMode().equals(IContainerInfo.INetworkMode.HOST);
        boolean discoveryFile = props.getProperty(IKeys.DISCOVERY_TYPE).equals("file");

        if (props.hasProperty(IKeys.CRYPTO_SECRET)) {
            var path = new File(props.getProperty(IKeys.CRYPTO_SECRET));
            if (!path.isFile()) {
                throw new IPropertyException(IKeys.CRYPTO_SECRET, path + " not found error");
            }
            LOGGER.info(IKeys.CRYPTO_SECRET + " set");
        } else {
            LOGGER.info(IKeys.CRYPTO_SECRET + " not set");
        }

        if (!hostNetwork) {
            props.setProperty(IProperties.join(IKeys.DRIVER_PORTS, "tcp", props.getString(IKeys.DRIVER_HEALTHCHECK_PORT)), "0");
            List<String> ports = new ArrayList<>();
            if (props.hasProperty(IProperties.join(IKeys.PORT, "host"))) {
                ports.addAll(props.getStringList(IProperties.join(IKeys.PORT, "host")));
            }
            for (int i = 0; i < props.getInteger(IKeys.TRANSPORT_PORTS); i++) {
                ports.add("0");
            }
            props.setProperty(IProperties.join(IKeys.PORT, "host"), String.join(",", ports));
        }
        if (interactive || staticConfig != null) {
            keyPair = ICrypto.genKeyPair();
            if (staticConfig != null) {
                props.setProperty(IKeys.DRIVER_PRIVATE_KEY, keyPair.privateKey());
            }
            props.setProperty(IKeys.DRIVER_PUBLIC_KEY, keyPair.publicKey());
        }

        if (interactive) {
            if (discoveryFile) {
                fileDiscovery();
            } else {
                etcdDiscovery();
            }
            driverArgs.add("ignis-sshserver");
            driverArgs.add("driver");
            if (hostNetwork) {
                driverArgs.add("0");
            } else {
                driverArgs.add(props.getString(IKeys.PORT));
                props.setProperty(IProperties.join(IKeys.DRIVER_PORTS, "tcp", IKeys.PORT), "0");
            }
        } else {
            driverArgs.add("ignis-run");
            driverArgs.add(cmd);
            if (args != null) {
                driverArgs.addAll(args);
            }
        }

        props.setProperty(IProperties.join(IKeys.DRIVER_ENV, IProperties.asEnv(IKeys.OPTIONS)), props.store64());
        var driver = schedulerParser.parse(IKeys.DRIVER, driverArgs);
        schedulerParser.containerEnvironment(driver, true, interactive, staticConfig != null);
        var executors = new IClusterRequest[0];

        if (staticConfig != null) {
            var execArgs = new ArrayList<String>();
            execArgs.add("ignis-sshserver");
            execArgs.add("executor");
            if (hostNetwork) {
                execArgs.add("0");
            } else {
                execArgs.add(props.getString(IKeys.PORT));
                props.setProperty(IProperties.join(IKeys.EXECUTOR_PORTS, "tcp", IKeys.PORT), "0");
            }
            if (staticConfig.equals("-")) {
                executors = new IClusterRequest[]{schedulerParser.parse(IKeys.EXECUTOR, execArgs)};
                schedulerParser.containerEnvironment(executors[0], false, interactive, true);
            } else {
                var multiple = props.multiLoad(staticConfig);
                executors = new IClusterRequest[multiple.size()];
                for (int i = 0; i < executors.length; i++) {
                    var parser = new ISchedulerParser(multiple.get(i));
                    executors[i] = parser.parse(IKeys.EXECUTOR, execArgs);
                    schedulerParser.containerEnvironment(executors[i], false, interactive, true);
                }
            }
        }

        synchronized (this) {
            if (Boolean.getBoolean(IKeys.DEBUG)) {
                LOGGER.info(props.toString());
                LOGGER.info("Driver Request: {" + ISchedulerUtils.yaml(driver) + "\n}\n");
                for (var e : executors) {
                    LOGGER.info("Executor Request: {" + ISchedulerUtils.yaml(e) + "\n}\n");
                }
            }

            String jobID = scheduler.createJob(props.getProperty(IKeys.JOB_NAME), driver, executors);
            if (interactive) {
                System.exit(connect(jobID, driverArgs));
            } else {
                System.out.println("submitted job " + jobID);
            }
        }
    }

    private void etcdDiscovery() {
        try {
            String token = ISchedulerUtils.genId() + "-driver";
            var endpoint = props.getProperty(IKeys.DISCOVERY_ETCD_ENDPOINT);
            var ca = props.getString(IKeys.DISCOVERY_ETCD_CA, null);
            var builder = Client.builder().waitForReady(false).endpoints(endpoint);
            var user = props.getProperty(IKeys.DISCOVERY_ETCD_USER, null);
            var password = props.getProperty(IKeys.DISCOVERY_ETCD_PASSWORD, null);
            props.setProperty(IKeys.DISCOVERY_TARGET, endpoint + (endpoint.endsWith("/") ? "" : "/") + token);
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
                    var key = ByteSequence.from(token.getBytes());
                    synchronized (this) {
                        var watch = client.getWatchClient().watch(key,
                                watchResponse -> {
                                    for (var event : watchResponse.getEvents()) {
                                        if (event.getEventType() == WatchEvent.EventType.PUT) {
                                            try {
                                                parent.interactivePort =
                                                        Integer.parseInt(event.getKeyValue().getValue().toString());
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
                                    parent.interactivePort =
                                            Integer.parseInt(values.get(0).getValue().toString());
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

    private void fileDiscovery() {
        String token = ISchedulerUtils.genId() + "-driver";
        var filename = new File(props.getProperty(IKeys.WORKING_DIRECTORY), "." + token).getPath();
        var delete = new Thread(() -> new File(filename).delete());
        Runtime.getRuntime().addShutdownHook(delete);
        props.setProperty(IKeys.DISCOVERY_TARGET, filename);
        Thread.ofPlatform().name("discovery").daemon().start(() -> {
            while (true) {
                var file = new File(filename);
                if (file.exists()) {
                    try {
                        var port = Files.readString(file.toPath());
                        if (port.endsWith("\n")) {
                            this.interactivePort = Integer.parseInt(port);
                            new File(filename).delete();
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
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
    }

    private int connect(String jobID, List<String> args) throws Exception {
        var cancel = new Thread(() -> {
            try {
                scheduler.cancelJob(jobID);
            } catch (ISchedulerException ex) {
                LOGGER.error(ex.getMessage(), ex);
            }
        });
        Runtime.getRuntime().addShutdownHook(cancel);
        synchronized (this) {
            try {
                this.wait();
                if (interactivePort == null) {
                    System.exit(-1);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        var jsch = new JSch();
        var job = scheduler.getJob(jobID);
        IContainerInfo driver = null;
        for (var cluster : job.clusters()) {
            if (cluster.name().equals(props.getProperty(IKeys.DRIVER_NAME))) {
                driver = cluster.containers().get(0);
                break;
            }
        }
        var user = driver.user().split(":")[0];
        this.wait();//Port Wait
        if (interactivePort == null) {
            throw new RuntimeException("Port error");
        }
        var session = jsch.getSession(user, driver.node(), interactivePort);
        session.setConfig("StrictHostKeyChecking", "no");
        jsch.addIdentity(user, keyPair.privateKey().getBytes(), keyPair.publicKey().getBytes(), null);
        session.connect(60000);
        var channel = (ChannelExec) session.openChannel("exec");
        channel.setOutputStream(System.out, true);
        channel.setErrStream(System.err, true);
        channel.setInputStream(System.in, true);

        channel.setCommand(
                args.stream().map((a) -> '"' + a.replace("\"", "\\\"") + '"').collect(Collectors.joining(" "))
        );
        channel.connect();
        while (!channel.isClosed()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }
        channel.disconnect();
        session.disconnect();
        Runtime.getRuntime().removeShutdownHook(cancel);
        return channel.getExitStatus();
    }

}
