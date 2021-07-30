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
 * @author César Pomar
 */
public final class ISendFileTask extends IContainerTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ISendFileTask.class);

    private final String source;
    private final String target;
    private int attempt;

    public ISendFileTask(String name, IContainer container, String source, String target) {
        super(name, container);
        this.source = source;
        this.target = target;
    }

    @Override
    public void run(ITaskContext context) throws IgnisException {
        if (attempt == container.getResets()) {
            return;
        }

        LOGGER.info(log() + "Sending file" + source + " to " + target);
        container.getTunnel().sendFile(source, target);
        LOGGER.info(log() + "File sent");
        attempt = container.getResets();
    }

}
