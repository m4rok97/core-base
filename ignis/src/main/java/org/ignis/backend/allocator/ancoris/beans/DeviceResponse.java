package org.ignis.backend.allocator.ancoris.beans;

/**
 *
 * @author César Pomar
 */
public final class DeviceResponse extends DeviceBase {

    private String id;

    public DeviceResponse() {
    }

    public String getId() {
        return id;
    }

    protected void setId(String id) {
        this.id = id;
    }

    protected void setGroup(String group) {
        this.group = group;
    }

    protected void setModel(String model) {
        this.model = model;
    }

    protected void setUnits(Integer units) {
        this.units = units;
    }

}
