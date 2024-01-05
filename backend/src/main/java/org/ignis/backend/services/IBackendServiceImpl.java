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

import com.sun.net.httpserver.HttpServer;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import org.ignis.properties.IKeys;
import org.ignis.rpc.driver.IBackendService;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.Executors;

/**
 * @author César Pomar
 */
public final class IBackendServiceImpl extends IService implements IBackendService.Iface {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IBackendServiceImpl.class);
    private TServer server;
    private HttpServer healthEndpoint;

    public IBackendServiceImpl(IServiceStorage ss) {
        super(ss);
    }

    public void start(TProcessor processor) {
        LOGGER.info("Backend server started");
        driverHealthCheck();
        TServerSocket transport = null;
        try {
            startHealthServer();
            var path = Path.of(ss.props().getProperty(IKeys.JOB_SOCKETS), "backend.sock");
            var compression = ss.props().getInteger(IKeys.TRANSPORT_COMPRESSION);
            var socket = new ServerSocket();
            socket.bind(UnixDomainSocketAddress.of(path));
            socket.setSoTimeout(0);
            transport = new TServerSocket(socket);
            server = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                    .protocolFactory(new TCompactProtocol.Factory())
                    .transportFactory(new TTransportFactory() {
                        @Override
                        public TTransport getTransport(TTransport base) {
                            try {
                                return new TZlibTransport(base, compression);
                            } catch (TTransportException ex) {
                                return null;//never happens
                            }
                        }
                    })
                    .executorService(Executors.newVirtualThreadPerTaskExecutor())
                    .processor(processor));
            ss.props().setReadOnly(true);
            server.serve();
        } catch (TTransportException | IOException ex) {
            LOGGER.error("Backend server fails", ex);
        }
        if (transport != null) {
            transport.close();
        }
        LOGGER.info("Backend server stopped");
    }

    @Override
    public void stop() throws TException {
        Thread.startVirtualThread(() -> {
            try {
                Thread.sleep(20000);//wait driver disconnection
            } catch (InterruptedException ex) {
            }
            stopAll();
        });
    }

    private void stopAll() {
        LOGGER.info("Stopping Backend server");
        try {
            stopHealthServer();
            server.stop();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    private void driverHealthCheck() {
        Thread.startVirtualThread(() -> {
            try {
                System.in.read();
            } catch (Exception ex) {
            }
            stopAll();
        });
    }

    private void startHealthServer() throws IOException {
        if (ss.props().hasProperty(IKeys.HEALTHCHECK_DISABLE)) {
            return;
        }
        int port = ss.isHostNetwork() ? 0 : ss.props().getInteger(IKeys.HEALTHCHECK_PORT);

        healthEndpoint = HttpServer.create(new InetSocketAddress(port), 0);
        healthEndpoint.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, -1);
        });
        healthEndpoint.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        healthEndpoint.start();
        LOGGER.info("Backend health server started");

        if (port == 0) {
            port = healthEndpoint.getAddress().getPort();
        } else {
            ss.driver().getInfo().hostPort(port);
        }

        String url = "http://" + ss.driver.getInfo().node() + ":" + port;
        ss.props().setProperty(IKeys.HEALTHCHECK_URL, url);
    }

    private void stopHealthServer() {
        try {
            if (healthEndpoint != null) {
                healthEndpoint.stop(0);
                LOGGER.info("Backend health server stopped");
            }
        } catch (Exception ex) {
        }
    }

}
