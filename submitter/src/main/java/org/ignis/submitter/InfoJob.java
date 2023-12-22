package org.ignis.submitter;

import picocli.CommandLine;

@CommandLine.Command(name = "info", helpCommand = true, description = "Get job info")
public class InfoJob extends BaseJob {

    @CommandLine.Parameters(paramLabel = "id", description = "job id")
    private String id;

    @Override
    public void run() throws Exception {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
