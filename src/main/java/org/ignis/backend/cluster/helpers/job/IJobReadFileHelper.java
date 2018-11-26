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
package org.ignis.backend.cluster.helpers.job;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.ignis.backend.cluster.IData;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.IJob;
import org.ignis.backend.cluster.tasks.TaskScheduler;
import org.ignis.backend.cluster.tasks.executor.IReadFileTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IJobReadFileHelper extends IJobHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IJobReadFileHelper.class);

    public IJobReadFileHelper(IJob job, IProperties properties) {
        super(job, properties);
    }

    private int[] distribute(int elemens, int boxs) {
        int[] distribution = new int[boxs + 1];
        distribution[0] = 0;
        int size = elemens / boxs;
        int mod = elemens % boxs;
        for (int i = 0; i < boxs; i++) {
            if (i < mod) {
                distribution[i + 1] = distribution[i] + size + 1;
            } else {
                distribution[i + 1] = distribution[i] + size;
            }
        }
        return distribution;
    }

    private long getIndex(InputStream buffer, long last, int from, int to) {
        long index = 0;
        int i = 0;
        try {
            for (int b = buffer.read(); b != -1; b = buffer.read()) {
                index |= (b & 127) << (7 * i);
                i++;
                if ((b & 128) == 0) {
                    last += index;
                    if(++from == to){
                        return last;
                    }
                    i = 0;
                    index = 0;
                }
            }
        } catch (IOException ex) {
        }
        return last;
    }

    private int countIndex(byte[] bytes) {
        int count = 0;
        for (int i = 0; i < bytes.length; i++) {
            if ((bytes[i] & 128) == 0) {
                count++;
            }
        }
        return count;
    }

    private byte[] fileIndex(String path) throws IgnisException {
        File pathFile = new File(path);
        if (!pathFile.exists()) {
            throw new IgnisException(path + " doesn't exist");
        }
        if (!pathFile.isFile()) {
            throw new IgnisException(path + " is not a file");
        }
        File indexFile = new File(path + ".ii");
        if (!indexFile.exists()) {
            try {
                int exit = Runtime.getRuntime().exec(new String[]{"ii", path}).waitFor();
                if (exit != 0) {
                    throw new IgnisException("Failed to index " + path + " exitcode " + exit);
                }
            } catch (IOException | InterruptedException ex) {
                throw new IgnisException("Failed to index " + path, ex);
            }
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream((int) indexFile.length());
        try (FileInputStream in = new FileInputStream(indexFile)) {
            int len;
            byte[] buffer = new byte[1024];
            while ((len = in.read(buffer)) > 0) {
                bytes.write(buffer, 0, len);
            }
        } catch (IOException ex) {
            throw new IgnisException("Failed to open index " + path + ".ii", ex);
        }
        return bytes.toByteArray();
    }

    public IData readFile(String path) throws IgnisException {
        LOGGER.info(log() + "Preparing readFile");
        byte[] bytes = fileIndex(path);
        InputStream buffer = new ByteArrayInputStream(bytes);
        int executors = job.getExecutors().size();
        int[] distribution = distribute(countIndex(bytes), executors);

        List<IExecutor> result = new ArrayList<>();
        TaskScheduler.Builder shedulerBuilder = new TaskScheduler.Builder(job.getLock());
        shedulerBuilder.newDependency(job.getScheduler());

        long last = 0;
        for (int i = 0; i < executors; i++) {
            IExecutor executor = job.getExecutors().get(i);
            int lines = distribution[i + 1] - distribution[i];
            long offset = last;
            last = getIndex(buffer, last, distribution[i] - 1, distribution[i + 1] - 1);
            long length = last - offset;
            shedulerBuilder.newTask(new IReadFileTask(this, executor, path, offset, length, lines));
            result.add(executor);
            LOGGER.info(log() + "Partition " + i + " lines:" + lines + ", offset: " + offset + ", length: " + length);
        }
        IData target = job.newData(result, shedulerBuilder.build());
        LOGGER.info(log() + "ReadFile path: " + path + " -> " + target.getName());
        return target;
    }

}
