package org.ignis.submitter;


import picocli.CommandLine;

@CommandLine.Command(name = "cancel", helpCommand = true, description = "Cancel a job")
public class CancelJob extends BaseJob {

    @CommandLine.Parameters(index = "0", paramLabel = "id", description = "job id", arity = "1")
    private String id;

    @Override
    public void run() throws Exception {
        scheduler.cancelJob(id);
    }
}
