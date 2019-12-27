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
package org.ignis.backend.cluster.tasks.container;

import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.ITaskContext;
import org.ignis.backend.exception.IgnisException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class ISendCompressedFileTask extends IContainerTask {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ISendCompressedFileTask.class);

    private final String source;
    private final String target;
    private int attempt;

    public ISendCompressedFileTask(String name, IContainer container, String source, String target) {
        super(name, container);
        this.source = source;
        this.target = target;
    }

    private final static String EXTRACTING_SCRIPT
            = "#!/bin/bash\n"
            + "if [ -f {file} ] ; then\n"
            + "  case {file} in\n"
            + "    *.tar.bz2)  tar xjf {file}    ;;\n"
            + "    *.tar.gz)   tar xzf {file}    ;;\n"
            + "    *.tar.xz)   tar zxvf {file}   ;;\n"
            + "    *.bz2)      bunzip2 {file}    ;;\n"
            + "    *.rar)      rar x {file}      ;;\n"
            + "    *.gz)       gunzip {file}     ;;\n"
            + "    *.tar)      tar xf {file}     ;;\n"
            + "    *.tbz2)     tar xjf {file}    ;;\n"
            + "    *.tgz)      tar xzf {file}    ;;\n"
            + "    *.xz)       xz -d {file}      ;;\n"
            + "    *.zip)      unzip {file}      ;;\n"
            + "    *.Z)        uncompress {file} ;;\n"
            + "    *)          echo 'contents of {file} cannot be extracted' ;;\n"
            + "  esac\n"
            + "else\n"
            + "  echo '{file} is not recognized as a compressed file'\n"
            + "fi";

    @Override
    public void run(ITaskContext context) throws IgnisException {
        if (attempt == container.getResets()) {
            return;
        }
        LOGGER.info(log() + "Sending file" + source + " to " + target);
        container.getTunnel().sendFile(source, target);
        LOGGER.info(log() + "File sent, extracting");
        container.getTunnel().execute(EXTRACTING_SCRIPT.replace("{file}", target));
        LOGGER.info(log() + "File extracted successfully");
        attempt = container.getResets();
    }

}
