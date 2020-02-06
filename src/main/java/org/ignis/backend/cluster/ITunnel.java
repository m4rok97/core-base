/*
 * Copyright (C) 2019 César Pomar
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
package org.ignis.backend.cluster;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.ignis.backend.exception.IgnisException;

/**
 *
 * @author César Pomar
 */
public final class ITunnel {

    private final JSch jsch;
    private final AtomicInteger localPort;
    private final Map<Integer, Integer> ports;
    private final String privateKey;
    private final String publicKey;
    private Session session;
    private int remotePort;

    public ITunnel(AtomicInteger localPort, int remotePortInit, String privateKey, String publicKey) {
        this.localPort = localPort;
        this.remotePort = remotePortInit;
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.jsch = new JSch();
        this.ports = new HashMap<>();
    }

    public String getPublicKey() {
        return publicKey;
    }

    public void open(String host, int port) throws IgnisException {
        for (int i = 0; i < 10; i++) {
            try {
                if (session != null) {
                    try {
                        close();
                    } catch (Exception ex) {
                    }
                }
                session = jsch.getSession("root", host, port);
                jsch.addIdentity("root", privateKey.getBytes(), publicKey.getBytes(), null);
                for (Map.Entry<Integer, Integer> entry : ports.entrySet()) {
                    session.setPortForwardingL(entry.getValue(), session.getHost(), entry.getKey());
                }
                session.connect();
                break;
            } catch (JSchException ex) {
                if (i == 9) {
                    throw new IgnisException(ex.getMessage(), ex);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex1) {
                    throw new IgnisException(ex.getMessage(), ex);
                }
            }
        }
    }

    public boolean test() {
        try {
            execute("cd .");
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    public void close() {
        session.disconnect();
    }

    public int registerPort() throws IgnisException {
        int newLocalPort = localPort.getAndIncrement();
        int newRemotePort = remotePort++;
        ports.put(newRemotePort, newLocalPort);
        return newLocalPort;
    }

    public String execute(List<String> cmds) throws IgnisException {
        StringBuilder builder = new StringBuilder();
        for (String cmd : cmds) {
            builder.append("#!/bin/bash\n");
            builder.append('"');
            builder.append(cmd.replace("\"", "\\\""));
            builder.append('"');
            builder.append("  2>&1");
        }
        return execute(builder.toString());
    }

    public String execute(String script) throws IgnisException {
        try {
            Channel channel = session.openChannel("shell");
            channel.connect();
            PrintStream stream = new PrintStream(channel.getOutputStream(), false, StandardCharsets.UTF_8.name());
            stream.println(script);
            stream.println("");
            stream.println("exit $?");
            stream.flush();

            StringBuilder out = new StringBuilder();
            byte[] buffer = new byte[256];
            int bytes;

            while ((bytes = channel.getInputStream().read(buffer)) > 0) {
                out.append(new String(buffer, 0, bytes, StandardCharsets.UTF_8));
            }

            channel.disconnect();

            if (channel.getExitStatus() == 0) {
                return out.toString();
            } else {
                String error = "\t" + script.replace("\n", "\t\n");

                throw new IgnisException("Script:\n" + error + " exits with non zero exit status "
                        + "(" + channel.getExitStatus() + ") and output: " + out);
            }

        } catch (IOException | JSchException ex) {
            throw new IgnisException("Script execution fails", ex);
        }
    }

    public void sendFile(String source, String target) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
