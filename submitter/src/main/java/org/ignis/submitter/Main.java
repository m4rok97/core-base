package org.ignis.submitter;

import picocli.CommandLine;

@CommandLine.Command(versionProvider = Main.Version.class, mixinStandardHelpOptions = true,
        subcommands = {RunJob.class, CancelJob.class, InfoJob.class, ListJobs.class})
public class Main  {

    public static class Version implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() {
            String v = this.getClass().getPackage().getImplementationVersion();
            if (v == null) {
                v = "dev";
            }
            return new String[]{v};
        }
    }

    public static void main(String[] args) {
        System.out.println("Executing main file of submitter");
        CommandLine cli = new CommandLine(Main.class)
                .setCommandName("ignis-submit")
                .setUsageHelpAutoWidth(true);
        System.exit(cli.execute(args));
    }


}
