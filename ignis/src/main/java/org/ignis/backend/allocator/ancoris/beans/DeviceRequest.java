package org.ignis.backend.allocator.ancoris.beans;

/**
 *
 * @author César Pomar
 */
public final class DeviceRequest extends DeviceBase {

    public DeviceRequest() {
    }

    public DeviceRequest setGroup(String group) {
        this.group = group;
        return this;
    }

    public DeviceRequest setModel(String model) {
        this.model = model;
        return this;
    }

    public DeviceRequest setUnits(Integer units) {
        this.units = units;
        return this;
    }

}
