package org.ignis.backend.allocator.ancoris.beans;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author César Pomar
 */
public final class ResourceRequest extends ResourceBase {

    private List<Integer> ports;

    public ResourceRequest() {
        volumes = new ArrayList<>();
        devices = new ArrayList<>();
        ports = new ArrayList<>();
    }

    public ResourceRequest setCores(Integer cores) {
        this.cores = cores;
        return this;
    }

    public ResourceRequest setMemory(String memory) {
        this.memory = memory;
        return this;
    }

    public ResourceRequest setSwap(String swap) {
        this.swap = swap;
        return this;
    }

    public ResourceRequest setVolumes(List<VolumeBase> volumes) {
        if (volumes != null) {
            this.volumes = volumes;
        }
        return this;
    }

    public ResourceRequest setDevices(List<DeviceBase> devices) {
        if (devices != null) {
            this.devices = devices;
        }
        return this;
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public ResourceRequest setPorts(List<Integer> ports) {
        if (ports != null) {
            this.ports = ports;
        }
        return this;
    }

    @Override
    public List<VolumeRequest> getVolumes() {
        return (List) volumes;
    }

    @Override
    public List<DeviceRequest> getDevices() {
        return (List) devices;
    }

}
