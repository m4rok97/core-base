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
package org.ignis.backend;

import java.io.ByteArrayInputStream;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.action.AppendArgumentAction;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.ignis.backend.exception.ISchedulerException;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.scheduler.IScheduler;
import org.ignis.backend.scheduler.ISchedulerBuilder;
import static org.ignis.backend.scheduler.ISchedulerParser.parseBinds;
import static org.ignis.backend.scheduler.ISchedulerParser.parseEnv;
import static org.ignis.backend.scheduler.ISchedulerParser.parseNetwork;
import static org.ignis.backend.scheduler.ISchedulerParser.parseVolumes;
import org.ignis.backend.scheduler.model.IContainerDetails;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class Submit {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Submit.class);

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("ignis-submit").build();
        Namespace ns = null;
        try {

            parser.addArgument("image").help("Driver container image");
            parser.addArgument("cmd").help("Driver executable");
            parser.addArgument("args").nargs("*").help("Driver executable arguments");

            parser.addArgument("--name").metavar("str").help("Job name");
            parser.addArgument("-p", "--property").metavar("key=value").action(new AppendArgumentAction())
                    .type((ArgumentParser ap, Argument arg, String value) -> {
                        String array[] = value.split("=");
                        if (array.length != 2) {
                            throw new ArgumentParserException("malformed property '" + value + "'", parser, arg);
                        }
                        return array;
                    }).help("Job properties");
            parser.addArgument("-pf", "--property-file").metavar("file").help("Job properties file");
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException ex) {
            if (args.length == 0) {
                parser.printHelp();
            } else {
                System.err.println(ex.getLocalizedMessage());
                parser.printUsage();
            }
            System.exit(-1);
        }

        try {
            IProperties props = new IProperties();
            props.fromEnv(System.getenv());
            if (props.contains(IKeys.OPTIONS)) {
                props.load(new ByteArrayInputStream(props.getProperty(IKeys.OPTIONS).getBytes()));
            }

            props.setProperty(IKeys.DRIVER_IMAGE, ns.getString("image"));
            if (ns.get("property-file") != null) {
                props.load("property-file");
            }
            if (ns.get("property") != null) {
                for (String[] entry : ns.<String[]>getList("property")) {
                    props.setProperty(entry[0], entry[1]);
                }
            }

            
            IScheduler scheduler = ISchedulerBuilder.create(props.getProperty(IKeys.SCHEDULER_TYPE),
                    props.getProperty(IKeys.SCHEDULER_URL));

            IContainerDetails.IContainerDetailsBuilder builder = IContainerDetails.builder();
            builder.image(props.getProperty(IKeys.DRIVER_IMAGE));
            builder.cpus(props.getInteger(IKeys.DRIVER_CORES));
            builder.memory((long) Math.ceil(props.getSILong(IKeys.DRIVER_MEMORY) / 1024 / 1024));
            builder.command(ns.getString("cmd"));
            builder.arguments(ns.getList("args"));
            builder.network(parseNetwork(props, IKeys.DRIVER_PORT));
            builder.binds(parseBinds(props, IKeys.DRIVER_BIND));
            builder.volumes(parseVolumes(props, IKeys.DRIVER_VOLUME));
            builder.environmentVariables(parseEnv(props, IKeys.DRIVER_ENV));
            if (props.contains(IKeys.DRIVER_HOSTS)) {
                builder.preferedHosts(props.getStringList(IKeys.DRIVER_HOSTS));
            }

            String group = null;
            try {
                group = scheduler.createGroup(ns.get("name") != null ? ns.get("name") : "ignis");
                props.setProperty(IKeys.GROUP, group);
                scheduler.createSingleContainer(group, "driver", builder.build(), props);
            } catch (ISchedulerException ex) {
                if (group != null) {
                    scheduler.destroyGroup(group);
                }
                throw ex;
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getLocalizedMessage());
        }

    }

}
