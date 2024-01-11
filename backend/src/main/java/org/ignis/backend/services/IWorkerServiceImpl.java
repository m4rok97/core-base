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
package org.ignis.backend.services;

import org.apache.thrift.TException;
import org.ignis.backend.cluster.ICluster;
import org.ignis.backend.cluster.IDataFrame;
import org.ignis.backend.cluster.IWorker;
import org.ignis.backend.cluster.helpers.worker.*;
import org.ignis.backend.cluster.tasks.ILazy;
import org.ignis.backend.cluster.tasks.ILock;
import org.ignis.backend.exception.IDriverExceptionImpl;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IKeys;
import org.ignis.rpc.ISource;
import org.ignis.rpc.driver.IDataFrameId;
import org.ignis.rpc.driver.IDriverException;
import org.ignis.rpc.driver.IWorkerId;
import org.ignis.rpc.driver.IWorkerService;

/**
 * @author César Pomar
 */
public final class IWorkerServiceImpl extends IService implements IWorkerService.Iface {

    public IWorkerServiceImpl(IServiceStorage ss) {
        super(ss);
    }

    @Override
    public IWorkerId newInstance(long id, String type) throws IDriverException, TException {
        return newInstance3(id, "", type);
    }

    @Override
    public void start(IWorkerId id) throws TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = worker.start();
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void destroy(IWorkerId id) throws TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = worker.destroy();
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IWorkerId newInstance3(long id, String name, String type) throws IDriverException, TException {
        try {
            int cores = ss.getCluster(id).getProperties().getInteger(IKeys.EXECUTOR_CORES);
            return newInstance5(id, name, type, cores, 1);
        } catch (IgnisException ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IWorkerId newInstance4(long id, String type, int cores, int instances) throws IDriverException, TException {
        return newInstance5(id, "", type, cores, instances);
    }

    @Override
    public IWorkerId newInstance5(long id, String name, String type, int cores, int instances) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id);
            synchronized (cluster.getLock()) {
                IWorker worker = cluster.createWorker(name, type, cores, instances);
                return new IWorkerId(id, worker.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void setName(IWorkerId id, String name) throws IDriverException, TException {
        try {
            ICluster clusterObject = ss.getCluster(id.getCluster());
            IWorker workerObject = clusterObject.getWorker(id.getWorker());
            synchronized (workerObject.getLock()) {
                workerObject.setName(name);
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId parallelize(IWorkerId id, long dataId, long partitions) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerParallelizeDataHelper(worker, cluster.getProperties()).parallelize(ss.driver, dataId, partitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }

    }

    @Override
    public IDataFrameId parallelize4(IWorkerId id, long dataId, long partitions, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerParallelizeDataHelper(worker, cluster.getProperties()).parallelize(ss.driver, dataId, partitions, src);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId importDataFrame(IWorkerId id, IDataFrameId data) throws IDriverException, TException {
        try {
            ICluster clusterSource = ss.getCluster(data.getCluster());
            ICluster clusterTarget = ss.getCluster(id.getCluster());
            IWorker workerSource = clusterSource.getWorker(data.getWorker());
            IWorker workerTarget = clusterTarget.getWorker(id.getWorker());

            ILock lock1 = workerSource.getLock();
            ILock lock2 = workerTarget.getLock();
            if (lock1.compareTo(lock2) < 0) {
                ILock tmp = lock1;
                lock1 = lock2;
                lock2 = tmp;
            }
            synchronized (lock1) {
                synchronized (lock2) {
                    IDataFrame source = workerSource.getDataFrame(data.getDataFrame());
                    IDataFrame target = new IWorkerImportDataHelper(workerTarget, clusterTarget.getProperties()).importDataFrame(source);
                    return new IDataFrameId(clusterTarget.getId(), workerTarget.getId(), target.getId());
                }
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId importDataFrame3(IWorkerId id, IDataFrameId data, ISource src) throws IDriverException, TException {
        try {
            ICluster clusterSource = ss.getCluster(data.getCluster());
            ICluster clusterTarget = ss.getCluster(id.getCluster());
            IWorker workerSource = clusterSource.getWorker(data.getWorker());
            IWorker workerTarget = clusterTarget.getWorker(id.getWorker());

            ILock lock1 = workerSource.getLock();
            ILock lock2 = workerTarget.getLock();
            if (lock1.compareTo(lock2) < 0) {
                ILock tmp = lock1;
                lock1 = lock2;
                lock2 = tmp;
            }
            synchronized (lock1) {
                synchronized (lock2) {
                    IDataFrame source = workerSource.getDataFrame(data.getDataFrame());
                    IDataFrame target = new IWorkerImportDataHelper(workerTarget, clusterTarget.getProperties()).importDataFrame(source, src);
                    return new IDataFrameId(clusterTarget.getId(), workerTarget.getId(), target.getId());
                }
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId plainFile(IWorkerId id, String path, String delim) throws IDriverException,TException{
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerReadFileHelper(worker, worker.getProperties()).plainFile(path, delim);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId plainFile4(IWorkerId id, String path, long minPartitions, String delim) throws IDriverException, TException{
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerReadFileHelper(worker, worker.getProperties()).plainFile(path, minPartitions, delim);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId textFile(IWorkerId id, String path) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerReadFileHelper(worker, worker.getProperties()).textFile(path);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId textFile3(IWorkerId id, String path, long partitions) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerReadFileHelper(worker, worker.getProperties()).textFile(path, partitions);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId partitionObjectFile(IWorkerId id, String path) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerReadFileHelper(worker, worker.getProperties()).partitionObjectFile(path);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId partitionObjectFile3(IWorkerId id, String path, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerReadFileHelper(worker, worker.getProperties()).partitionObjectFile(path, src);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId partitionTextFile(IWorkerId id, String path) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerReadFileHelper(worker, worker.getProperties()).partitionTextFile(path);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId partitionJsonFile3a(IWorkerId id, String path, boolean objectMapping) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerReadFileHelper(worker, worker.getProperties()).partitionJsonFile(path, objectMapping);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId partitionJsonFile3b(IWorkerId id, String path, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerReadFileHelper(worker, worker.getProperties()).partitionJsonFile(path, src);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void loadLibrary(IWorkerId id, String path) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = new IWorkerLoadLibraryHelper(worker, worker.getProperties()).loadLibrary(path);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void execute(IWorkerId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = new IWorkerExecuteHelper(worker, worker.getProperties()).execute(src);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId executeTo(IWorkerId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerExecuteHelper(worker, worker.getProperties()).executeTo(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void voidCall(IWorkerId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            ILazy<Void> result;
            synchronized (worker.getLock()) {
                result = new IWorkerExecuteHelper(worker, worker.getProperties()).execute(src);
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public void voidCall3(IWorkerId id, IDataFrameId data, ISource src) throws IDriverException, TException {
        try {
            ICluster clusterSource = ss.getCluster(data.getCluster());
            ICluster clusterTarget = ss.getCluster(id.getCluster());
            IWorker workerSource = clusterSource.getWorker(data.getWorker());
            IWorker workerTarget = clusterTarget.getWorker(id.getWorker());

            ILock lock1 = workerSource.getLock();
            ILock lock2 = workerTarget.getLock();
            if (lock1.compareTo(lock2) < 0) {
                ILock tmp = lock1;
                lock1 = lock2;
                lock2 = tmp;
            }
            ILazy<Void> result;
            synchronized (lock1) {
                synchronized (lock2) {
                    IDataFrame source = workerSource.getDataFrame(data.getDataFrame());
                    result = new IWorkerCallHelper(workerTarget, workerTarget.getProperties()).voidCall(source, src);
                }
            }
            result.execute();
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId call(IWorkerId id, ISource src) throws IDriverException, TException {
        try {
            ICluster cluster = ss.getCluster(id.getCluster());
            IWorker worker = cluster.getWorker(id.getWorker());
            synchronized (worker.getLock()) {
                IDataFrame data = new IWorkerCallHelper(worker, worker.getProperties()).call(src);
                return new IDataFrameId(cluster.getId(), worker.getId(), data.getId());
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }

    @Override
    public IDataFrameId call3(IWorkerId id, IDataFrameId data, ISource src) throws IDriverException, TException {
        try {
            ICluster clusterSource = ss.getCluster(data.getCluster());
            ICluster clusterTarget = ss.getCluster(id.getCluster());
            IWorker workerSource = clusterSource.getWorker(data.getWorker());
            IWorker workerTarget = clusterTarget.getWorker(id.getWorker());

            ILock lock1 = workerSource.getLock();
            ILock lock2 = workerTarget.getLock();
            if (lock1.compareTo(lock2) < 0) {
                ILock tmp = lock1;
                lock1 = lock2;
                lock2 = tmp;
            }
            synchronized (lock1) {
                synchronized (lock2) {
                    IDataFrame source = workerSource.getDataFrame(data.getDataFrame());
                    IDataFrame target = new IWorkerCallHelper(workerTarget, workerTarget.getProperties()).call(source, src);
                    return new IDataFrameId(clusterTarget.getId(), workerTarget.getId(), target.getId());
                }
            }
        } catch (Exception ex) {
            throw new IDriverExceptionImpl(ex);
        }
    }
}
