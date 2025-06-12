package org.ignis.submitter;

import org.ignis.scheduler.ISchedulerUtils;
import picocli.CommandLine;

@CommandLine.Command(name = "info", helpCommand = true, description = "Get job info")
public class InfoJob extends BaseJob {

    @CommandLine.Parameters(paramLabel = "id", description = "job id")
    private String id;

    @CommandLine.Option(names = {"-f", "--field"}, paramLabel = "str", description = "show only a field of the job")
    private String field;

    @Override
    public void run() throws Exception {
        var info = scheduler.getJob(id);
        if (field != null) {
            System.out.println(ISchedulerUtils.yaml(info, field));
        } else {
            System.out.println(ISchedulerUtils.yaml(info));
        }
    }
}
