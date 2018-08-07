package org.ignis.backend.allocator.ancoris.beans;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author César Pomar
 */
public final class OptionsRequest extends OptionsBase {

    public OptionsRequest() {
        prefered_hosts = new ArrayList<>();
    }

    public OptionsRequest setReplicas(Integer replicas) {
        this.replicas = replicas;
        return this;
    }

    public OptionsRequest setPrefered_hosts(List<String> prefered_hosts) {
        if (prefered_hosts != null) {
            this.prefered_hosts = prefered_hosts;
        }
        return this;
    }

    public OptionsRequest setSwappiness(String swappiness) {
        this.swappiness = swappiness;
        return this;
    }

}
