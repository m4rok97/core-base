/*
 * Copyright (C) 2018 
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
public final class ILock implements Comparable<ILock> {

    private final long id;
    private final long weak;//If worker has his own Lock will be weak

    public ILock(long id) {
        this(id, -1);
    }

    public ILock(long id, long weak) {
        this.id = id;
        this.weak = weak;
    }

    @Override
    public int compareTo(ILock lock) {
        long cmp = id - lock.id;
        if (cmp > 0) {
            return 1;
        } else if (cmp < 0) {
            return -1;
        }
        cmp = weak - lock.weak;
        if (cmp >= 0) {
            return 1;
        }
        return -1;
    }

    @Override
    public String toString() {
        return String.valueOf(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj != null && obj instanceof ILock) {
            return id == ((ILock) obj).id && weak == ((ILock) obj).weak;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

}
