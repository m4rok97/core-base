package org.ignis.backend.allocator.ancoris.beans;

import java.util.List;

/**
 *
 * @author César Pomar
 */
public abstract class OptionsBase {

    protected Integer replicas;
    protected List<String> prefered_hosts;
    protected String swappiness;

    public OptionsBase() {
    }

    public Integer getReplicas() {
        return replicas;
    }

    public List<String> getPrefered_hosts() {
        return prefered_hosts;
    }

    public String getSwappiness() {
        return swappiness;
    }

}
