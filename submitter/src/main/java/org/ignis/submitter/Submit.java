/*
 * Copyright (C) 2019 César Pomar
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
package org.ignis.submitter;

import org.ignis.properties.IPropertyException;
import org.ignis.scheduler.ISchedulerException;
import org.ignis.properties.IKeys;
import org.ignis.properties.IProperties;
import org.ignis.scheduler.IScheduler;
import org.ignis.scheduler.ISchedulerBuilder;
import org.ignis.properties.IPropetiesParser;
import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IContainerStatus;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Callable;

/**
 * @author César Pomar
 */
public class Submit implements Callable<Integer> {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Submit.class);

    @Parameters(index = "0", paramLabel = "image", description = "Driver container image")
    private String image;

    @Parameters(index = "1", paramLabel = "cmd", description = "Driver executable", preprocessor = ConsumeArgs.class)
    private String cmd;

    @Parameters(index = "2", paramLabel = "args", arity = "*", description = "Driver executable arguments")
    private List<String> args;

    @Option(names = {"--name"}, paramLabel = "NAME", description = "Job name")
    private String name;

    @Option(names = {"-p", "--property"}, paramLabel = "<key=value>", description = "Job properties")
    Map<String, String> userProperties;

    @Option(names = {"-f", "--property-file"}, paramLabel = "FILE", description = "Job properties file")
    String userPropertiesFile;

    @Option(names = {"--direct"}, description = "Execute cmd directly without ignis-run")
    boolean direct = false;

    @Option(names = {"--attach"}, description = "Attach to the Ignis HPC task")
    boolean attach = false;

    @Option(names = {"--print-id"}, description = "Print Ignis HPC task id")
    boolean print_id = false;

    @Option(names = {"--wait"}, description = "Wait until Ignis HPC task finalization")
    boolean wait = false;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    @Override
    public Integer call() throws Exception {
        try {
            IProperties defaults = new IProperties();
            IProperties props = new IProperties(defaults);
            props.fromEnv(System.getenv());

            try {
                String conf = new File(props.getString(IKeys.HOME), "etc/ignis.conf").getPath();
                defaults.load(conf);
            } catch (IPropertyException | IOException ex) {
                LOGGER.error("Error loading ignis.conf, ignoring", ex);
            }

            props.setProperty(IKeys.DRIVER_IMAGE, image);
            if (userPropertiesFile != null) {
                props.load(userPropertiesFile);
            }
            if (userProperties != null) {
                props.fromMap(userProperties);
            }
            ByteArrayOutputStream options = new ByteArrayOutputStream();
            props.store(options);
            if (props.contains(IKeys.DEBUG) && props.getBoolean(IKeys.DEBUG)) {
                System.setProperty(IKeys.DEBUG, "true");
                LOGGER.info("DEBUG enabled");
            } else {
                System.setProperty(IKeys.DEBUG, "false");
            }

            IScheduler scheduler = ISchedulerBuilder.create(props.getProperty(IKeys.SCHEDULER_TYPE),
                    props.getProperty(IKeys.SCHEDULER_URL));

            IContainerInfo.IContainerInfoBuilder builder = IContainerInfo.builder();
            if (props.contains(IKeys.REGISTRY)) {
                String registry = props.getProperty(IKeys.REGISTRY);
                if (!registry.endsWith("/")) {
                    registry += "/";
                }
                builder.image(registry + props.getProperty(IKeys.DRIVER_IMAGE));
            } else {
                builder.image(props.getProperty(IKeys.DRIVER_IMAGE));
            }
            builder.cpus(props.getInteger(IKeys.DRIVER_CORES));
            builder.memory((long) Math.ceil(props.getSILong(IKeys.DRIVER_MEMORY) / 1024 / 1024));
            builder.shm(props.contains(IKeys.DRIVER_SHM) ? (long) Math.ceil(props.getSILong(IKeys.DRIVER_SHM) / 1024 / 1024) : null);
            builder.swappiness(props.contains(IKeys.DRIVER_SWAPPINESS) ? props.getInteger(IKeys.DRIVER_SWAPPINESS) : null);
            List<IPort> ports;
            builder.ports(ports = IPropetiesParser.parsePorts(props, IKeys.DRIVER_PORT));
            ports.add(new IPort(props.getInteger(IKeys.DRIVER_HEALTHCHECK_PORT), 0, "tcp"));
            builder.binds(IPropetiesParser.parseBinds(props, IKeys.DRIVER_BIND));
            builder.volumes(IPropetiesParser.parseVolumes(props, IKeys.DRIVER_VOLUME));
            builder.hostnames(props.getStringList(IKeys.SCHEDULER_DNS));
            Map<String, String> env = IPropetiesParser.parseEnv(props, IKeys.DRIVER_ENV);
            env.put("IGNIS_OPTIONS", options.toString());//Send submit options to driver            
            builder.environmentVariables(env);
            if (props.contains(IKeys.DRIVER_HOSTS)) {
                builder.preferedHosts(props.getStringList(IKeys.DRIVER_HOSTS));
            }

            if (!props.contains(IKeys.WORKING_DIRECTORY)) {
                props.setProperty(IKeys.WORKING_DIRECTORY, props.getProperty(IKeys.DFS_HOME));
            }

            if (direct) {
                builder.command(cmd);
                builder.arguments(args);
            } else {
                env.put("IGNIS_WORKING_DIRECTORY", props.getProperty(IKeys.WORKING_DIRECTORY));
                builder.command("ignis-run");
                List<String> arguments = new ArrayList<>();
                arguments.add(cmd);
                if (args != null) {
                    arguments.addAll(args);
                }
                builder.arguments(arguments);
            }

            String group = null;
            String app;
            try {
                group = scheduler.createGroup(name != null ? name : "ignis");
                props.setProperty(IKeys.JOB_GROUP, group);
                app = scheduler.createDriverContainer(group, "driver", builder.build());
            } catch (ISchedulerException ex) {
                if (group != null) {
                    scheduler.destroyGroup(group);
                }
                throw ex;
            }

            if(print_id){
                System.out.println(props.getProperty(IKeys.SCHEDULER_TYPE) + ":" + app);
            }

            if (attach) {
                throw new UnsupportedOperationException("Not supported yet.");
            } else if (wait) {
                scheduler.getContainer(app);
                while (true) {
                    IContainerStatus status = scheduler.getStatus(app);
                    LOGGER.info("Task status is " + status.name());
                    if (status != IContainerStatus.ACCEPTED &&
                            status != IContainerStatus.RUNNING) {
                        if (status == IContainerStatus.FINISHED) {
                            return 0;
                        } else {
                            return -1;
                        }
                    }
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                    }
                }
            }

        } catch (Exception ex) {
            LOGGER.error(ex.getLocalizedMessage(), ex);
            return -1;
        }
        return 0;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        CommandLine cli = new CommandLine(new Submit())
                .setCommandName("ignis-submit")
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

}
