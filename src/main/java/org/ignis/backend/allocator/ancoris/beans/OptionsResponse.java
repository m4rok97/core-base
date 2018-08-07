package org.ignis.backend.allocator.ancoris.beans;

import java.util.Collections;
import java.util.List;

/**
 *
 * @author César Pomar
 */
public final class OptionsResponse extends OptionsBase {

    public OptionsResponse() {
    }

    protected void setReplicas(Integer replicas) {
        this.replicas = replicas;
    }

    protected void setPrefered_hosts(List<String> prefered_hosts) {
        this.prefered_hosts = Collections.unmodifiableList(prefered_hosts);
    }

    protected void setSwappiness(String swappiness) {
        this.swappiness = swappiness;
    }

}
