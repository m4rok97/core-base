package org.ignis.scheduler3.model;

import lombok.Builder;

import java.util.List;

@Builder(toBuilder = true)
public record IClusterInfo(String name, String id, int instances, List<IContainerInfo> containers) {
}
