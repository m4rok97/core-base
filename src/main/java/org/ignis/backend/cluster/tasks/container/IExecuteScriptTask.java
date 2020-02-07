/*
 * Copyright (C) 2018 
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either attempt 3 of the License, or
 * (at your option) any later attempt.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.ignis.backend.cluster.tasks.container;

import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IExecuteScriptTask extends IContainerTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IExecuteScriptTask.class);

    private final String script;
    private int attempt;

    public IExecuteScriptTask(String name, IContainer container, String script) {
        super(name, container);
        this.script = script;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        if (attempt == container.getResets()) {
            return;
        }
        LOGGER.info(log() + "Executing script:\n\t" + script.replace("\n", "\t\n"));
        container.getTunnel().execute(script, true);
        LOGGER.info(log() + "Script executed");
        attempt = container.getResets();
    }

}
