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
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IKeys;
import org.slf4j.LoggerFactory;

/**
 * @author César Pomar
 */
public final class ITunnel {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ITunnel.class);

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
                session.setConfig("StrictHostKeyChecking", "no");
                jsch.addIdentity("root", privateKey.getBytes(), publicKey.getBytes(), null);
                for (Map.Entry<Integer, Integer> entry : ports.entrySet()) {
                    session.setPortForwardingL(entry.getKey(), "localhost", entry.getValue());
                }
                session.connect();
                break;
            } catch (JSchException ex) {
                if (i == 9) {
                    throw new IgnisException("Could not connect to " + host + ":" + port, ex);
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
            execute("cd .", false);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    public void close() {
        if (session != null) {
            for (Integer port : ports.keySet()) {
                try {
                    session.delPortForwardingL(port);
                } catch (JSchException e) {
                }
            }
            session.disconnect();
        }
    }

    public int registerPort() throws IgnisException {
        int newLocalPort = localPort.incrementAndGet();
        int newRemotePort = remotePort++;
        ports.put(newLocalPort, newRemotePort);
        if (session != null) {
            try {
                session.setPortForwardingL(newLocalPort, "localhost", newRemotePort);
            } catch (JSchException ex) {
            }
        }
        return newLocalPort;
    }

    public int getRemotePort(int port) {
        return ports.get(port);
    }

    public String execute(List<String> cmds) throws IgnisException {
        StringBuilder builder = new StringBuilder();
        for (String cmd : cmds) {
            builder.append('"');
            builder.append(cmd.replace("\"", "\\\""));
            builder.append('"').append(' ');
        }
        return execute(builder.toString(), false);
    }

    public String execute(String script, boolean stderr) throws IgnisException {
        try {
            Channel channel = session.openChannel("exec");
            channel.setInputStream(null);
            if (stderr) {
                script = "exec 2>&1\n" + script;
            }
            ((ChannelExec) channel).setCommand(script);

            channel.connect();

            InputStream in = channel.getInputStream();
            StringBuilder out = new StringBuilder();

            byte[] buffer = new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(buffer, 0, buffer.length);
                    if (i < 0) {
                        break;
                    }
                    out.append(new String(buffer, 0, i, StandardCharsets.UTF_8));
                }
                if (channel.isClosed()) {
                    if (in.available() > 0) {
                        continue;
                    }
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {
                }
            }
            channel.disconnect();

            if (Boolean.getBoolean(IKeys.DEBUG)) {
                LOGGER.info("Debug: Script: \n\t" + script.replace("\n", "\n\t"));
                LOGGER.info("Debug: Script output: " + out.toString());
            }

            if (channel.getExitStatus() == 0) {
                return out.toString();
            } else {
                String error = "\t" + script.replace("\n", "\n\t");

                LOGGER.error("Script:\n" + error + " exits with non zero exit status "
                        + "(" + channel.getExitStatus() + ") and output: " + out);

                throw new IgnisException("Script exits with non zero exit status (" + channel.getExitStatus() + ")");
            }

        } catch (IOException | JSchException ex) {
            throw new IgnisException("Script execution fails", ex);
        }
    }

    public void sendFile(String source, String target) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
