package org.ignis.scheduler;

class ContainerId {

    private String jobId;
    private String taskGroup;
    private int instance;

    ContainerId(String containerId) {
        int sep1 = containerId.indexOf(".");
        int sep2 = containerId.indexOf("[", sep1);
        int sep3 = containerId.indexOf("]", sep2);
        jobId = containerId.substring(0, sep1);
        taskGroup = containerId.substring(sep1 + 1, sep2);
        instance = Integer.parseInt(containerId.substring(sep2 + 1, sep3));
    }

    ContainerId(String jobId, String taskGroup) {
        this(jobId, taskGroup, 0);
    }

    ContainerId(String jobId, String taskGroup, int instance) {
        this.jobId = jobId;
        this.taskGroup = taskGroup;
        this.instance = instance;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getTaskGroup() {
        return taskGroup;
    }

    public void setTaskGroup(String taskGroup) {
        this.taskGroup = taskGroup;
    }

    public int getInstance() {
        return instance;
    }

    public void setInstance(int instance) {
        this.instance = instance;
    }

    @Override
    public String toString() {
        return jobId + "." + taskGroup + "[" + instance + "]";
    }
}
