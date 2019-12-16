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

import java.util.Arrays;
import java.util.List;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.action.AppendArgumentAction;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.ignis.backend.properties.IKeys;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.scheduler.IMarathonScheduler;
import org.ignis.backend.scheduler.model.IJobContainer;
import org.ignis.backend.scheduler.model.INetwork;
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
        IJobContainer container = IJobContainer.builder().
                cpus(1).memory(100l).image("ubuntu:18.04").command("sleep").arguments(Arrays.asList("infinity"))
                .network(new INetwork()).build();
        container.getNetwork().getTcpMap().put(61751, 0);
        IMarathonScheduler scheduler = new IMarathonScheduler("http://localhost:8080");
        String group = scheduler.createGroup("grupo");
        List<String> id = scheduler.createContainerIntances(group, "hola", container, null, 2);
        scheduler.getContainer(id.get(0));
        scheduler.restartContainer(id.get(0));

        //args = new String[]{"-p","a=b","-p","a=b","image","cmd"};
        ArgumentParser parser = ArgumentParsers.newFor("ignis-submit").build();
        Namespace ns = null;
        try {

            parser.addArgument("image").help("Driver container image");
            parser.addArgument("cmd").help("Driver executable");
            parser.addArgument("args").nargs("*").help("Driver executable arguments");

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
            props.setProperty(IKeys.DRIVER_IMAGE, ns.getString("image"));
            if (ns.get("property-file") != null) {
                props.load("property-file");
            }
            if (ns.get("property") != null) {
                for (String[] entry : ns.<String[]>getList("property")) {
                    props.setProperty(entry[0], entry[1]);
                }
            }

        } catch (Exception ex) {
            LOGGER.error(ex.getLocalizedMessage());
        }

    }

}
