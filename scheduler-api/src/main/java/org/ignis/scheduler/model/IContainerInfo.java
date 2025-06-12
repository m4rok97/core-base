package org.ignis.scheduler.model;

import lombok.Builder;

import java.util.List;
import java.util.Map;

@Builder(toBuilder = true)
public record IContainerInfo(
        String id,
        String node,
        String image,
        List<String> args,
        int cpus,
        String gpu,
        long memory,//Bytes
        Long time,//Seconds
        String user,//name:UID:GUID
        boolean writable,
        boolean tmpdir,
        List<IPortMapping> ports,
        List<IBindMount> binds,
        List<String> nodelist,
        Map<String, String> hostnames,
        Map<String, String> env,
        INetworkMode network,
        IStatus status,
        IProvider provider,
        Map<String, String> schedulerOptArgs
) {
    public enum IStatus {
        ACCEPTED,
        RUNNING,
        ERROR,
        FINISHED,
        DESTROYED,
        UNKNOWN
    }

    public enum INetworkMode {
        HOST, BRIDGE
    }

    public enum IProvider {
        DOCKER, SINGULARITY, APPTAINER
    }

    public int hostPort(int n) {
        for (var port : ports) {
            if (n == port.container()) {
                return port.host();
            }
        }
        return -1;
    }


}
