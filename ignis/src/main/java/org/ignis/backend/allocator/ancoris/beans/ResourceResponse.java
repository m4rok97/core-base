package org.ignis.backend.allocator.ancoris.beans;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author César Pomar
 */
public final class ResourceResponse extends ResourceBase {

    private Map<Integer, PortResponse> ports;

    public ResourceResponse() {
    }

    protected void setCores(Integer cores) {
        this.cores = cores;
    }

    protected void setMemory(String memory) {
        this.memory = memory;
    }

    protected void setSwap(String swap) {
        this.swap = swap;
    }

    protected void setVolumes(List<VolumeResponse> volumes) {
        this.volumes = Collections.unmodifiableList(volumes);
    }

    protected void setDevices(List<DeviceResponse> devices) {
        this.devices = Collections.unmodifiableList(devices);
    }

    protected void setPorts(List<PortResponse> ports) {
        this.ports = new HashMap<>(ports.size());
        for (PortResponse port : ports) {
            this.ports.put(port.getContainer(), port);
        }
        this.ports = Collections.unmodifiableMap(this.ports);
    }

    protected Collection<PortResponse> getPorts() {
        return ports.values();
    }

    public Map<Integer, PortResponse> getPortsMap() {
        return ports;
    }

    @Override
    public List<VolumeResponse> getVolumes() {
        return (List) volumes;
    }

    @Override
    public List<DeviceResponse> getDevices() {
        return (List) devices;
    }

}
