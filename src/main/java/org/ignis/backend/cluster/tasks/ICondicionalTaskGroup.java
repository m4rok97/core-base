/*
 *
 *  * Copyright (C) 2021 César Pomar
 *  *
 *  * This program is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * This program is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package org.ignis.backend.cluster.tasks;

import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IgnisException;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class ICondicionalTaskGroup extends ITaskGroup {

    public static class Builder extends ITaskGroup.Builder {
        private final Supplier<Boolean> cond;

        public Builder(Supplier<Boolean> cond) {
            this.cond = cond;
        }

        public Builder(ILock lock, Supplier<Boolean> cond) {
            super(lock);
            this.cond = cond;
        }

        @Override
        public ITaskGroup build() {
            return new ICondicionalTaskGroup(tasks, locks, depencencies, cond);
        }
    }

    private final Supplier<Boolean> cond;

    protected ICondicionalTaskGroup(List<ITask> tasks, Set<ILock> locks, List<ITaskGroup> depencencies, Supplier<Boolean> cond) {
        super(tasks, locks, depencencies);
        this.cond = cond;
    }

    @Override
    protected void start(IThreadPool pool, List<ILock> locks, ITaskContext context) throws IgnisException {
        if (locks.isEmpty()) {
            if (cond.get()) {
                super.start(pool, locks, context);
            }
        } else {
            super.start(pool, locks, context);
        }
    }
}
