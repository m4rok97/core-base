/*
 * Copyright (C) 2019 César Pomar
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.ignis.backend.cluster.tasks;

/**
 *
 * @author César Pomar
 */
public class ICache {

    public static byte NO_CACHE = 0;
    public static byte PRESERVE = 1;
    public static byte MEMORY = 2;
    public static byte RAW_MEMORY = 3;
    public static byte DISK = 4;

    private final long id;
    private byte level;
    private boolean cached;

    public ICache(long id) {
        this.id = id;
        level = 0;
        cached = false;
    }

    public long getId() {
        return id;
    }

    public byte getLevel() {
        return level;
    }

    public void setLevel(byte level) {
        this.level = level;
    }

    public boolean isCached() {
        return cached;
    }

    public void setCached(boolean cached) {
        this.cached = cached;
    }

}
