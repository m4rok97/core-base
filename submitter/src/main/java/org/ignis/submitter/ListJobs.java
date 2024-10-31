package org.ignis.submitter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ignis.scheduler3.model.IContainerInfo;
import org.ignis.scheduler3.model.IJobInfo;

import picocli.CommandLine;

@CommandLine.Command(name = "list", helpCommand = true, description = "Display jobs")
public class ListJobs extends BaseJob {

    @CommandLine.Option(names = { "-f",
            "--field" }, paramLabel = "str", description = "show only a specific field of the jobs")
    private String field;

    @CommandLine.Option(names = { "-l",
            "--filter" }, paramLabel = "key=value", description = "filter jobs by labels (e.g. 'key=value')")
    private Map<String, String> filters = new HashMap<>();

    @Override
    public void run() throws Exception {
        List<IJobInfo> jobs = scheduler.listJobs(filters);

        System.out.printf("%-20s %-20s %-15s %-10s\n", "JOB ID", "NAME", "STATUS", "INSTANCES");
        System.out.println("-------------------------------------------------------------------");

        for (IJobInfo job : jobs) {
            String jobId = job.id();
            String jobName = job.name();

            String jobStatus = "N/A";
           
            IContainerInfo driver = null;
            for (var cluster : job.clusters()) {
                if (cluster.id().startsWith("0")) {
                    driver = cluster.containers().get(0);
                    break;
                }
            }
            
            jobStatus = driver.status().name();
            
            int totalInstances = job.clusters().isEmpty() ? 0 : job.clusters().get(0).instances();

            System.out.printf("%-20s %-20s %-15s %-10d\n", jobId, jobName, jobStatus, totalInstances);
        }
    }
}