package org.ignis.submitter;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.properties.IPropertyException;
import org.ignis.scheduler.ISchedulerParser;
import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IPort;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

@CommandLine.Command(version = "2.2")
public class SlurmSubmit implements Callable<Integer> {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SlurmSubmit.class);

    @Parameters(index = "0", paramLabel = "time", description = "Set a limit on the total run time of the job allocation")
    private String time;

    @Parameters(index = "1", paramLabel = "image", description = "Driver container image")
    private String image;

    @Parameters(index = "2", paramLabel = "cmd", description = "Driver executable", preprocessor = ConsumeArgs.class)
    private String cmd;

    @Parameters(index = "3", paramLabel = "args", arity = "*", description = "Driver executable arguments")
    private List<String> args;

    @Option(names = {"--name"}, paramLabel = "NAME", description = "Job name", defaultValue = "ignis")
    private String name;

    @Option(names = {"-p", "--property"}, paramLabel = "<key=value>", description = "Job properties")
    Map<String, String> userProperties;

    @Option(names = {"-f", "--property-file"}, paramLabel = "FILE", description = "Job properties file")
    String userPropertiesFile;

    @Option(names = {"--init-port"}, paramLabel = "n", description = "If Slurm does not reserve ports, this will be the " +
            "initial port on each node, the others will be consecutive.")
    Integer initPort;

    @Option(names = {"--num-ports"}, paramLabel = "n", description = "If Slurm reserve ports, number of ports to be " +
            "reserved for the job on each node. (default 10)", defaultValue = "10")
    Integer numPorts;

    @Option(names = {"-s", "--slurm-arg"}, paramLabel = "--arg=value", description = "Slurm argument", preprocessor = SlurmConsumeArg.class)
    List<String> slurmArgs;

    @Option(names = {"--slurm-init"}, paramLabel = "[--arg=value]... --slurm-end", description = "Slurm argument group", preprocessor = SlurmArgsGroup.class)
    List<String> slurmGroup;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    @Override
    public Integer call() throws Exception {
        try {
            IProperties defaults = new IProperties();
            IProperties props = new IProperties(defaults);
            String home = System.getProperty("user.home");
            props.setProperty(IKeys.EXECUTOR_IMAGE, image);
            if(home!= null) {
                props.setProperty(IKeys.DFS_ID, System.getProperty("user.home"));
            }
            props.fromEnv(System.getenv());

            defaults.load(getClass().getClassLoader().getResourceAsStream("etc/ignis.conf"));
            try {
                File conf;
                if(props.contains(IKeys.HOME)) {
                    conf = new File(props.getString(IKeys.HOME), "etc/ignis.conf");
                    if (conf.exists()) {
                        props.load(conf.getPath());
                    }
                }
                if(home != null){
                    conf = new File(System.getProperty("user.home"), ".ignis/ignis.conf");
                    if (conf.exists()) {
                        props.load(conf.getPath());
                    }
                }
            } catch (IPropertyException | IOException ex) {
                LOGGER.error("Error loading ignis.conf, ignoring", ex);
            }

            deployProperties(props);

            props.setProperty(IKeys.DRIVER_IMAGE, image);
            if (userPropertiesFile != null) {
                props.load(userPropertiesFile);
            }
            if (userProperties != null) {
                props.fromMap(userProperties);
            }

            if (props.contains(IKeys.DEBUG) && props.getBoolean(IKeys.DEBUG)) {
                System.setProperty(IKeys.DEBUG, "true");
                LOGGER.info("DEBUG enabled");
            } else {
                System.setProperty(IKeys.DEBUG, "false");
            }

            ByteArrayOutputStream privateKeyBuff = new ByteArrayOutputStream(2048);
            ByteArrayOutputStream publicKeyBuff = new ByteArrayOutputStream(2048);
            try {
                KeyPair keyPair = KeyPair.genKeyPair(new JSch(), KeyPair.RSA, 2048);
                keyPair.writePrivateKey(privateKeyBuff);
                keyPair.writePublicKey(publicKeyBuff, "");
            } catch (JSchException ex) {
            }
            props.setProperty(IKeys.DRIVER_PRIVATE_KEY, privateKeyBuff.toString());
            props.setProperty(IKeys.DRIVER_PUBLIC_KEY, publicKeyBuff.toString());

            StringBuilder customArgs = new StringBuilder();

            if (initPort != null) {
                props.setProperty(IKeys.SCHEDULER_PARAMS + ".port", initPort.toString());
            }

            if (slurmArgs != null) {
                for (String arg : slurmArgs) {
                    customArgs.append(arg).append(' ');
                }
            }

            if (slurmGroup != null) {
                for (String arg : slurmGroup) {
                    customArgs.append(arg).append(' ');
                }
            }

            if (!props.contains(IKeys.WORKING_DIRECTORY)) {
                props.setProperty(IKeys.WORKING_DIRECTORY, props.getProperty(IKeys.DFS_HOME));
            }

            IContainerInfo driver = parse(props, true);
            IContainerInfo executor = parse(props, false);
        /*TODO new common API
            Slurm scheduler = new Slurm(props.getProperty(IKeys.SCHEDULER_URL));
            String hostWd = props.getString(IKeys.DFS_ID);
            scheduler.createJob(time, name, customArgs.toString(), hostWd, driver, executor, props.getInteger(IKeys.EXECUTOR_INSTANCES));
        */
        } catch (Exception ex) {
            LOGGER.error(ex.getLocalizedMessage(), ex);
            return -1;
        }
        return 0;
    }

    /*
     * Propagates properties that would be created by the deploy
     * */
    private void deployProperties(IProperties props) {
        props.setProperty(IKeys.DFS_ID, props.getProperty(IKeys.DFS_ID));
        props.setProperty(IKeys.SCHEDULER_URL, props.getProperty(IKeys.SCHEDULER_URL, "sbatch"));
        props.setProperty(IKeys.SCHEDULER_TYPE, props.getProperty(IKeys.SCHEDULER_TYPE, "slurm"));
        if (props.contains(IKeys.REGISTRY)) {
            props.setProperty(IKeys.REGISTRY, props.getProperty(IKeys.REGISTRY));
        }
    }

    private IContainerInfo parse(IProperties props, boolean driver) throws Exception {
        IContainerInfo.IContainerInfoBuilder builder = IContainerInfo.builder();
        if (props.contains(IKeys.REGISTRY)) {
            String registry = props.getProperty(IKeys.REGISTRY);
            if (!registry.endsWith("/")) {
                registry += "/";
            }
            builder.image(registry + props.getProperty(driver ? IKeys.DRIVER_IMAGE : IKeys.EXECUTOR_IMAGE));
        } else {
            builder.image(props.getProperty(driver ? IKeys.DRIVER_IMAGE : IKeys.EXECUTOR_IMAGE));
        }
        builder.cpus(props.getInteger(driver ? IKeys.DRIVER_CORES : IKeys.EXECUTOR_CORES));
        builder.memory(props.getSILong(driver ? IKeys.DRIVER_MEMORY : IKeys.EXECUTOR_MEMORY));
        ISchedulerParser parser = new ISchedulerParser(props);
        builder.schedulerParams(parser.schedulerParams());

        List<IPort> ports = new ArrayList<>();
        int transPorts = props.getInteger(IKeys.TRANSPORT_PORTS);
        int totalPorts = transPorts + numPorts;
        ports.addAll(Collections.nCopies(totalPorts, new IPort(0, 0, "tcp")));
        builder.ports(ports);

        builder.binds(parser.binds(driver ? IKeys.DRIVER_BIND : IKeys.EXECUTOR_BIND));
        builder.volumes(parser.volumes(driver ? IKeys.DRIVER_VOLUME : IKeys.EXECUTOR_VOLUME));
        if (props.contains(IKeys.SCHEDULER_DNS)) {
            builder.hostnames(props.getStringList(IKeys.SCHEDULER_DNS));
        }
        Map<String, String> env = parser.env(driver ? IKeys.DRIVER_ENV : IKeys.EXECUTOR_ENV);
        if (driver) {
            env.put("IGNIS_OPTIONS", props.store64());//Send submit options to driver
            env.put("IGNIS_WORKING_DIRECTORY", props.getProperty(IKeys.WORKING_DIRECTORY));
        } else {
            env.put("IGNIS_DRIVER_PUBLIC_KEY", props.getProperty(IKeys.DRIVER_PUBLIC_KEY).replace("\n", ""));
        }

        builder.environmentVariables(env);
        if (props.contains(driver ? IKeys.DRIVER_HOSTS : IKeys.EXECUTOR_HOSTS)) {
            builder.preferedHosts(props.getStringList(driver ? IKeys.DRIVER_HOSTS : IKeys.EXECUTOR_HOSTS));
        }

        if (!driver) {
            builder.command("ignis-server");
            builder.arguments(List.of("`expr", "${SLURM_STEP_RESV_PORTS%%-*}", "+", transPorts + "`"));
        } else {
            builder.command("ignis-run");
            List<String> arguments = new ArrayList<>();
            arguments.add(cmd);
            if (args != null) {
                arguments.addAll(args);
            }
            builder.arguments(arguments);
        }

        return builder.build();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        CommandLine cli = new CommandLine(new SlurmSubmit())
                .setCommandName("Dockerfiles/slurm/ignis-slurm")
                .setUsageHelpAutoWidth(true);
        int exitCode = cli.execute(args);
        System.exit(exitCode);
    }

    static class ConsumeArgs implements CommandLine.IParameterPreprocessor {
        public boolean preprocess(Stack<String> args,
                                  CommandSpec commandSpec,
                                  ArgSpec argSpec,
                                  Map<String, Object> info) {
            String cmd = args.pop();
            if (!args.isEmpty()) {
                for (ArgSpec opt : commandSpec.positionalParameters()) {
                    if (opt.paramLabel().equals("args")) {
                        List<String> list = new ArrayList<>();
                        while (!args.isEmpty()) {
                            String arg = args.pop();
                            list.add(arg);
                        }
                        opt.setValue(list);
                        break;
                    }
                }
            }
            args.push(cmd);
            return false;
        }
    }

    static class SlurmArgsGroup implements CommandLine.IParameterPreprocessor {
        public boolean preprocess(Stack<String> args,
                                  CommandSpec commandSpec,
                                  ArgSpec argSpec,
                                  Map<String, Object> info) {
            if (!args.isEmpty()) {
                List<String> list = new ArrayList<>();
                while (!args.isEmpty()) {
                    String arg = args.pop();
                    if (arg.equals("--slurm-end")) {
                        argSpec.setValue(list);
                        return true;
                    }
                    list.add(arg);
                }
            }
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "--slurm-end not found");
        }
    }

    static class SlurmConsumeArg implements CommandLine.IParameterPreprocessor {
        public boolean preprocess(Stack<String> args,
                                  CommandSpec commandSpec,
                                  ArgSpec argSpec,
                                  Map<String, Object> info) {
            if (argSpec.getValue() == null) {
                argSpec.setValue(new ArrayList<>());
            }
            if (!args.isEmpty()) {
                ((List<String>) argSpec.getValue()).add(args.pop());
                return true;
            }
            return false;
        }
    }
}
