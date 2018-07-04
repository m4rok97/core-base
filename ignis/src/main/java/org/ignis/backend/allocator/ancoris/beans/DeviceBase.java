package org.ignis.backend.allocator.ancoris.beans;

/**
 *
 * @author César Pomar
 */
public abstract class DeviceBase {

    protected String group;
    protected String model;
    protected Integer units;

    public DeviceBase() {
    }

    public String getGroup() {
        return group;
    }

    public String getModel() {
        return model;
    }

    public Integer getUnits() {
        return units;
    }

}
