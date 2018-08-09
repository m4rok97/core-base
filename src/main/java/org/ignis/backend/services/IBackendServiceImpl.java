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
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.ignis.backend.exception.IgnisException;
import org.ignis.rpc.IRemoteException;
import org.ignis.rpc.driver.IBackendService;
import org.slf4j.LoggerFactory;

/**
 *
 * @author César Pomar
 */
public final class IBackendServiceImpl extends IService implements IBackendService.Iface {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IBackendServiceImpl.class);

    private TServerTransport transport;
    private TServer server;

    public IBackendServiceImpl(IAttributes attributes) {
        super(attributes);
    }

    public void start(TProcessor processor, int port) {
        LOGGER.info("Backend server started on port " + port);
        try {
            transport = new TServerSocket(port);
            server = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                    .protocolFactory(new TCompactProtocol.Factory())
                    .processor(processor));
            server.serve();
        } catch (TTransportException ex) {
            LOGGER.error("Backend server fails");
        }
        transport.close();
        LOGGER.info("Backend server stopped");
    }

    @Override
    public void stop() throws IRemoteException, TException {
        LOGGER.info("Stopping Backend server");
        try {
            server.stop();
        } catch (Exception ex) {
            throw new IgnisException(ex.getLocalizedMessage(), ex);
        }
    }

}
