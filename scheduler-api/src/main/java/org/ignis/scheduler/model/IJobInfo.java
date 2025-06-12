package org.ignis.scheduler.model;

import lombok.Builder;

import java.util.List;

@Builder(toBuilder = true)
public record IJobInfo(String name, String id, List<IClusterInfo> clusters) {
}
