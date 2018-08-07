package org.ignis.backend.allocator.ancoris.beans;

/**
 *
 * @author César
 */
public final class StatusResponse {

    public enum Status {
        SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED;

        static Status fromString(String value) {
            return Status.valueOf(value);
        }

    }

    private Status taskStatus;

    public StatusResponse() {
    }

    public Status getTaskStatus() {
        return taskStatus;
    }

    protected String getStatus() {
        return taskStatus.toString();
    }

    protected void setStatus(String status) {
        this.taskStatus = Status.valueOf(status);
    }

}
