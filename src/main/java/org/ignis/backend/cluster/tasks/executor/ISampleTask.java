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
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class ISampleTask extends IExecutorContextTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ISampleTask.class);

    private final boolean withReplacement;
    private final double fraction;
    private final int seed;

    public ISampleTask(String name, IExecutor executor, boolean withReplacement, double fraction, int seed) {
        super(name, executor, Mode.LOAD);
        this.withReplacement = withReplacement;
        this.fraction = fraction;
        this.seed = seed;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        //TODO
    }

}
