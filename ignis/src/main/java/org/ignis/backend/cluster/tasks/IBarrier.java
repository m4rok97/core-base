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

import java.util.concurrent.CyclicBarrier;

/**
 *
 * @author César Pomar
 */
public class IBarrier extends CyclicBarrier {

    /**
     * Creates a new {@code IBarrier} that will trip when the given number of parties (threads) are waiting upon
     * it, and does not perform a predefined action when the barrier is tripped.
     *
     * @param parties the number of threads that must invoke {@link #await} before the barrier is tripped
     * @throws IllegalArgumentException if {@code parties} is less than 1
     */
    public IBarrier(int parties) {
        super(parties);
    }

}
