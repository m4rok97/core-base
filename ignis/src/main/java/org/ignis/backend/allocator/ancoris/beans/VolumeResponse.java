package org.ignis.backend.allocator.ancoris.beans;

import java.util.List;

/**
 *
 * @author César Pomar
 */
public final class VolumeResponse extends VolumeBase {

    public VolumeResponse() {
    }

    protected void setId(String id) {
        this.id = id;
    }

    protected void setPath(String path) {
        this.path = path;
    }

    protected void setSize(String size) {
        this.size = size;
    }

    protected void setType(String type) {
        this.volumeType = Type.fromString(type);
    }

    protected void setMode(String mode) {
        this.volumeMode = Mode.fromString(mode);
    }

    protected void setGroups(List<String> groups) {
        this.groups = groups;
    }

}
