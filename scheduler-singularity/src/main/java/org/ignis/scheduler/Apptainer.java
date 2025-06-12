package org.ignis.scheduler;

/**
 * @author César Pomar
 * <p>
 * Scheduler parameters:
 * apptainer.cgroup=true : Allows to disable cgroup
 */
public final class Apptainer extends Singularity {

    public Apptainer(String binary) {
        super(binary == null ? "apptainer" : binary, "apptainer");
    }
}
