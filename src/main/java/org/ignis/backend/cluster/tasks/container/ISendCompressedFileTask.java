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

import java.nio.ByteBuffer;
import org.apache.thrift.TException;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.exception.IgnisException;

/**
 *
 * @author César Pomar
 */
public class ISendCompressedFileTask extends IContainerTask {

    private final String path;
    private final ByteBuffer bytes;

    public ISendCompressedFileTask(IContainer container, String path, ByteBuffer bytes) {
        super(container);
        this.path = path;
        this.bytes = bytes;
    }

    @Override
    public void execute() throws IgnisException {
        try {
            container.getFileManager().sendFileAndExtract(path, bytes);
        } catch (TException ex) {
            throw new IgnisException("Fails to send " + path, ex);
        }
    }

}
