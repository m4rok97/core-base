/*
 * Copyright (C) 2020 César Pomar
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

import org.apache.thrift.TException;
import org.ignis.backend.cluster.IExecutor;
import org.ignis.rpc.IExecutorException;
import org.ignis.rpc.ISource;

/**
 * @author César Pomar
 */
public final class IPartitionTextFileTask extends IPartitionFileTask {

    public IPartitionTextFileTask(String name, IExecutor executor, Shared shared, String path) {
        super("partitionTextFile", name, executor, shared, path);
    }

    @Override
    public void read(String path, long first, long partitions, ISource src) throws IExecutorException, TException {
    }

    @Override
    public void read(String path, long first, long partitions) throws IExecutorException, TException {
        executor.getIoModule().partitionTextFile(path, first, partitions);
    }

}
