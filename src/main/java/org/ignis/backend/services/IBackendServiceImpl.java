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

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TZlibTransport;
import org.ignis.backend.exception.IDriverExceptionImpl;
import org.ignis.backend.properties.IKeys;
import org.ignis.rpc.driver.IBackendService;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @author César Pomar
 */
public final class IBackendServiceImpl extends IService implements IBackendService.Iface {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IBackendServiceImpl.class);

    private TServerTransport transport;
    private TServer server;
    private HttpServer healthEndpoint;

    public IBackendServiceImpl(IAttributes attributes) {
        super(attributes);
    }

    public void start(TProcessor processor, int port, int compression) {
        LOGGER.info("Backend server started on port " + port);
        driverHealthCheck();
        startHealthServer();
        try {
            transport = new TServerSocket(port);
            server = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                    .protocolFactory(new TCompactProtocol.Factory())
                    .transportFactory(new TTransportFactory() {
                        @Override
                        public TTransport getTransport(TTransport base) {
                            return new TZlibTransport(base, compression);
                        }
                    })
                    .processor(processor));
            server.serve();
        } catch (TTransportException ex) {
            LOGGER.error("Backend server fails");
        }
        transport.close();
        LOGGER.info("Backend server stopped");
    }

    @Override
    public void stop() throws TException {
        new Thread(() -> {
            try {
                Thread.sleep(5000);//wait driver disconnection
            } catch (InterruptedException ex) {
            }
            stopAll();
        }).start();
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
        Thread lc = new Thread(() -> {
            try {
                System.in.read();
            } catch (Exception ex) {
            }
            stopAll();
        });
        lc.start();
    }

    private void startHealthServer() {
        try {
            int port = attributes.defaultProperties.getInteger(IKeys.DRIVER_HEALTHCHECK_PORT);
            healthEndpoint = HttpServer.create(new InetSocketAddress(port), 0);
            HttpContext context = healthEndpoint.createContext("/");
            context.setHandler(exchange -> {
                exchange.getResponseHeaders().add("Content-Type", "text/html");
                exchange.sendResponseHeaders(200, 2);
                try (var os = exchange.getResponseBody()) {
                    os.write("Ok".getBytes());
                }
            });
            healthEndpoint.start();
            LOGGER.info("Backend health server started");
        } catch (Exception ex) {
            LOGGER.info("Backend health server error");
        }
    }

    private void stopHealthServer() {
        try {
            if (healthEndpoint != null) {
                healthEndpoint.stop(0);
            }
            LOGGER.info("Backend health server stopped");
        } catch (Exception ex) {
        }
    }

}
