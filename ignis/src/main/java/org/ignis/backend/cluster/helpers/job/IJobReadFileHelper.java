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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.ignis.backend.cluster.IData;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.backend.cluster.IJob;
import org.ignis.backend.cluster.ISplit;
import org.ignis.backend.cluster.tasks.executor.IReadFileTask;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public class IJobReadFileHelper extends IJobHelper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IJobReadFileHelper.class);

    public IJobReadFileHelper(IJob job, IProperties properties) {
        super(job, properties);
    }

    private List<Long> parseIndex(String path) throws IgnisException {
        File pathFile = new File(path);
        if (!pathFile.exists()) {
            throw new IgnisException(path + " doesn't exist");
        }
        if (!pathFile.isFile()) {
            throw new IgnisException(path + " is not a file");
        }
        List<Long> indices = new ArrayList<>(100000);
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
        long offset = 0;
        try (FileInputStream in = new FileInputStream(indexFile);
                BufferedInputStream buf = new BufferedInputStream(in);) {
            long index = 0;
            int i = 0;
            for (int b = buf.read(); b != -1; b = buf.read()) {
                index |= (b & 127) << (7 * i);
                i++;
                if ((b & 128) == 0) {
                    indices.add(offset += index);
                    i = 0;
                    index = 0;
                }
            }
        } catch (IOException ex) {
            throw new IgnisException("Failed to open index " + path + ".ii", ex);
        }
        return indices;
    }

    public IData readFile(String path) throws IgnisException {
        List<Long> indices = parseIndex(path);
        int executors = job.getExecutors().size();
        long size = indices.size() / executors;
        long mod = indices.size() % executors;//TODO
/*
        List<ISplit> result = new ArrayList<>();
        for (int i = 0; i < executors; i++) {
            IExecutor executor = job.getExecutors().get(i);
            long offset = size * i + i < mod ? i : mod;
            long length = size * (i + 1) + (i + 1) < mod ? (i + 1) : mod;
            long lines = 0;

            result.add(new ISplit(executor, new IReadFileTask(executor, path, offset, length, lines, job.getLock())));
        }
        return new IData(job.getDataSize(), job, result);

        int[] indexs = new int[executors + 1];
        indexs[0] = 0;
        int size = elems / executors;
        int mod = elems % executors;
        for (int i = 0; i < executors; i++) {
            if (i < mod) {
                indexs[i + 1] = indexs[i] + size + 1;
            } else {
                indexs[i + 1] = indexs[i] + size;
            }
        }
        return indexs;*/

        return null;
    }

}
