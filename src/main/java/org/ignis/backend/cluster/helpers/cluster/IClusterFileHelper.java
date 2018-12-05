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
package org.ignis.backend.cluster.helpers.cluster;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.cluster.IContainer;
import org.ignis.backend.cluster.tasks.ITaskScheduler;
import org.ignis.backend.cluster.tasks.container.ISendCompressedFileTask;
import org.ignis.backend.cluster.tasks.container.ISendFilesTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IClusterFileHelper extends IClusterHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IClusterFileHelper.class);

    public IClusterFileHelper(ICluster cluster, IProperties properties) {
        super(cluster, properties);
    }

    private ByteBuffer loadFile(File file) throws IgnisException {
        LOGGER.info(log() + "Loading " + file.getPath() + " " + file.length() + " bytes");
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) file.length());
        try (FileInputStream in = new FileInputStream(file)) {
            IOUtils.copy(in, out);
        } catch (IOException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
        return ByteBuffer.wrap(out.toByteArray());
    }

    public int sendFiles(String source, String target) throws IgnisException {
        LOGGER.info(log() + "Loading files from " + source + " to " + target);
        File wd = new File(".");
        FileFilter fileFilter = new WildcardFileFilter(source);
        Map<String, ByteBuffer> files = new HashMap<>();
        for (File file : wd.listFiles(fileFilter)) {
            files.put(new File(target, file.getName()).getPath(), loadFile(file));
        }
        ITaskScheduler.Builder shedulerBuilder = new ITaskScheduler.Builder(cluster.getLock());
        shedulerBuilder.newDependency(cluster.getScheduler());
        for (IContainer container : cluster.getContainers()) {
            shedulerBuilder.newTask(new ISendFilesTask(this, container, files));
        }
        cluster.putScheduler(shedulerBuilder.build());
        return files.size();
    }

    public int sendCompressedFile(String source, String target) throws IgnisException {
        LOGGER.info(log() + "Loading compressed file from " + source + " to " + target);
        File file = new File(source);
        ITaskScheduler.Builder shedulerBuilder = new ITaskScheduler.Builder(cluster.getLock());
        shedulerBuilder.newDependency(cluster.getScheduler());
        for (IContainer container : cluster.getContainers()) {
            shedulerBuilder.newTask(new ISendCompressedFileTask(this, container,
                    new File(target, file.getName()).getPath(), loadFile(new File(source)))
            );
        }
        cluster.putScheduler(shedulerBuilder.build());
        return 1;
    }

}
