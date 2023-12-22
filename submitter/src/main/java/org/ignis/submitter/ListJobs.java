package org.ignis.submitter;

import picocli.CommandLine;

@CommandLine.Command(name = "list", helpCommand = true, description = "Display jobs")
public class ListJobs extends BaseJob {

    @Override
    public void run() throws Exception {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
