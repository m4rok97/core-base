package org.ignis.scheduler3.model;

import lombok.Builder;

@Builder(toBuilder = true)
public record IClusterRequest(String name, IContainerInfo resources, int instances) {
}
