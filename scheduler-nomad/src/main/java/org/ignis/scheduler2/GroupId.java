package org.ignis.scheduler2;

class GroupId {

    private String jobId;
    private String name;

    GroupId(String groupId) {
        int sep = groupId.indexOf("-");
        name = groupId.substring(0, sep);
        jobId = groupId.substring(sep + 1);
    }

    GroupId(String jobId, String name) {
        this.jobId = jobId;
        this.name = name;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name + "-" + jobId;
    }
}
