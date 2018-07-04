package org.ignis.backend.allocator.ancoris.beans;

import java.util.Collections;
import java.util.List;

/**
 *
 * @author César Pomar
 */
public final class VolumeRequest extends VolumeBase {

    public VolumeRequest() {
    }
    
    public VolumeRequest setId(String id) {
        this.id = id;
        return this;
    }

    public VolumeRequest setGroups(List<String> groups) {
        this.groups = Collections.unmodifiableList(groups);
        return this;
    }

    public VolumeRequest setPath(String path) {
        this.path = path;
        return this;
    }

    public VolumeRequest setSize(String size) {
        this.size = size;
        return this;
    }

    public VolumeRequest setVolumeType(Type type) {
        this.volumeType = type;
        return this;
    }

    public VolumeRequest setVolumeMode(Mode mode) {
        this.volumeMode = mode;
        return this;
    }

}
