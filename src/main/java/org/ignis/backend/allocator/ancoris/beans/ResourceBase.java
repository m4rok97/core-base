package org.ignis.backend.allocator.ancoris.beans;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author César Pomar
 */
public abstract class ResourceBase {

    protected Integer cores;
    protected String memory;
    protected String swap;
    protected List<VolumeBase> volumes;
    protected List<DeviceBase> devices;

    public ResourceBase() {
        volumes = new ArrayList<>();
        devices = new ArrayList<>();
    }

    public Integer getCores() {
        return cores;
    }

    public String getMemory() {
        return memory;
    }

    public String getSwap() {
        return swap;
    }

    public List<? extends VolumeBase> getVolumes() {
        return volumes;
    }

    public List<? extends DeviceBase> getDevices() {
        return devices;
    }

}
