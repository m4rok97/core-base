package org.ignis.scheduler.model;

import lombok.Builder;

@Builder(toBuilder = true)
public record IBindMount(String container, String host, boolean ro) {
}
