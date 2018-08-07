package org.ignis.backend.allocator.ancoris.beans;

/**
 *
 * @author César Pomar
 */
public final class PortResponse {

    private Integer container;
    private Integer host;

    public Integer getContainer() {
        return container;
    }

    protected void setContainer(Integer container) {
        this.container = container;
    }

    public Integer getHost() {
        return host;
    }

    protected void setHost(Integer host) {
        this.host = host;
    }

}
