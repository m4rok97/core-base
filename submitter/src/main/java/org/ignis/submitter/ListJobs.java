package org.ignis.submitter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ignis.scheduler3.ISchedulerUtils;
import org.ignis.scheduler3.model.IJobInfo;

import picocli.CommandLine;

@CommandLine.Command(name = "list", helpCommand = true, description = "Display jobs")
public class ListJobs extends BaseJob {

    @CommandLine.Option(names = {"-f", "--field"}, paramLabel = "str", description = "show only a specific field of the jobs")
    private String field;

    @CommandLine.Option(names = {"-l", "--filter"}, paramLabel = "key=value", description = "filter jobs by labels (e.g. 'key=value')")
    private Map<String, String> filters = new HashMap<>();

    @Override
    public void run() throws Exception {
        List<IJobInfo> jobs = scheduler.listJobs(filters);

        if (jobs.isEmpty()) {
            System.out.println("No jobs found.");
            return;
        }

        for (IJobInfo job : jobs) {
            if (field != null) {
                System.out.println(ISchedulerUtils.yaml(job, field));
            } else {
                System.out.println(ISchedulerUtils.yaml(job));
            }
        }
    }
}
