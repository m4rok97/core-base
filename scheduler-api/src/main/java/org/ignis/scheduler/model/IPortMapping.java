package org.ignis.scheduler.model;

import lombok.Builder;

@Builder(toBuilder = true)
public record IPortMapping(int container, int host, Protocol protocol) {

    public enum Protocol {
        TCP,
        UDP
    }

}
