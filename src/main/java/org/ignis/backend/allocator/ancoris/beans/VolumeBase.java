package org.ignis.backend.allocator.ancoris.beans;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;

/**
 *
 * @author César Pomar
 */
public abstract class VolumeBase {

    public static enum Type {
        HDD, SDD, TMPFS, DFS;

        static Type fromString(String value) {
            for (Type t : Type.values()) {
                if (t.toString().toLowerCase().equals(value)) {
                    return t;
                }
            }
            return DFS;
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    public enum Mode {
        RO, RW, RO_RO, RO_RW, RW_RO, RW_RW;

        static Mode fromString(String value) {
            return Mode.valueOf(value.replace('-', '_').toUpperCase());
        }

        @Override
        public String toString() {
            return super.toString().replace('_', '-').toLowerCase();
        }

    }

    protected String id;
    protected List<String> groups;
    protected String path;
    protected String size;
    protected Type volumeType;
    protected Mode volumeMode;

    public VolumeBase() {
    }

    public String getId() {
        return id;
    }

    public List<String> getGroup() {
        return groups;
    }

    public String getPath() {
        return path;
    }

    public String getSize() {
        return size;
    }

    protected String getType() {
        return volumeType != null ? volumeType.toString() : null;
    }

    protected String getMode() {
        return volumeMode != null ? volumeMode.toString() : null;
    }

    @JsonIgnore
    public Type getVolumeType() {
        return volumeType;
    }

    @JsonIgnore
    public Mode getVolumeMode() {
        return volumeMode;
    }

}
