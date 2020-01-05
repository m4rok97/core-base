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
package org.ignis.backend.cluster.tasks.executor;

import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.ISource;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IImportDataTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IImportDataTask.class);

    public static class Shared {

        public Shared(int sources, int targets, String commId) {
            this.sources = sources;
            this.targets = targets;
            this.commId = commId;
            barrier = new IBarrier(sources + targets);
        }

        private final int sources;
        private final int targets;
        private final String commId;
        private final IBarrier barrier;
    }

    private final Shared shared;
    private final boolean source;
    private final Long partitions;
    private final ISource src;

    public IImportDataTask(String name, IExecutor executor, Shared shared, boolean source, Long partitions, ISource src) {
        super(name, executor, source ? Mode.LOAD : Mode.SAVE);
        this.shared = shared;
        this.source = source;
        this.partitions = partitions;
        this.src = src;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        throw new UnsupportedOperationException("Not supported on this version."); //TODO next version
    }

}
