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

import com.jcraft.jsch.*;
import org.ignis.backend.exception.IgnisException;
import org.ignis.properties.IKeys;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author César Pomar
 */
public final class ITunnel {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ITunnel.class);

    private final JSch jsch;
    private final Semaphore sem;
    private final AtomicInteger localPort;
    private final Map<Integer, Integer> ports;
    private final String privateKey;
    private final String publicKey;
    private final boolean portForwarding;
    private Session session;
    private int remotePort;

    public ITunnel(AtomicInteger localPort, int remotePortInit, boolean portForwarding, String privateKey, String publicKey) {
        this.localPort = localPort;
        this.remotePort = remotePortInit;
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.portForwarding = portForwarding;
        this.jsch = new JSch();
        this.ports = new HashMap<>();
        this.sem = new Semaphore(10);//Maximum channels at the same time in a single session
    }

    public String getPublicKey() {
        return publicKey;
    }

    public void open(String host, int port) throws IgnisException {
        String user = System.getProperty("user.name", "root");
        for (int i = 0; i < 300; i++) {
            try {
                if (session != null) {
                    try {
                        close();
                    } catch (Exception ex) {
                    }
                }
                session = jsch.getSession(user, host, port);
                session.setConfig("StrictHostKeyChecking", "no");
                jsch.addIdentity(user, privateKey.getBytes(), publicKey.getBytes(), null);
                if (portForwarding){
                    for (Map.Entry<Integer, Integer> entry : ports.entrySet()) {
                        session.setPortForwardingL(entry.getKey(), "localhost", entry.getValue());
                    }
                }
                session.connect();
                break;
            } catch (JSchException ex) {
                if (i == 299) {
                    throw new IgnisException("Could not connect to " + user + " " + host + ":" + port, ex);
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
        int newRemotePort = remotePort++;
        int newLocalPort = portForwarding ? localPort.incrementAndGet() : newRemotePort;
        ports.put(newLocalPort, newRemotePort);
        if (session != null && portForwarding) {
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
            this.sem.acquire();
            Channel channel = session.openChannel("exec");
            String envScript = "source /ssh/environment && bash - << 'EOF'\n" + script + "\nEOF\n";

            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            ((ChannelExec) channel).setCommand(envScript);
            ((ChannelExec) channel).setInputStream(null);
            ((ChannelExec) channel).setOutputStream(buffer);
            if (stderr) {
                ((ChannelExec) channel).setErrStream(buffer);
            } else {
                ((ChannelExec) channel).setErrStream(new ByteArrayOutputStream());
            }

            channel.connect(60000);

            while(!channel.isClosed()){
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {
                }
            }
            channel.disconnect();
            String out = buffer.toString(StandardCharsets.UTF_8);

            if (Boolean.getBoolean(IKeys.DEBUG)) {
                LOGGER.info("Debug: Script: \n\t" + script.replace("\n", "\n\t"));
                LOGGER.info("Debug: Script output: \n\t" + out.replace("\n", "\n\t"));
            }

            if (channel.getExitStatus() == 0) {
                return out;
            } else {
                LOGGER.error("Script: \n\t" + script.replace("\n", "\n\t") +
                        " exits with non zero exit status "
                        + "(" + channel.getExitStatus() + ") and output: \n\t" +
                        out.replace("\n", "\n\t"));

                throw new IgnisException("Script exits with non zero exit status (" + channel.getExitStatus() + ")");
            }

        } catch (JSchException | InterruptedException ex) {
            throw new IgnisException("Script execution fails", ex);
        }finally {
            this.sem.release();
        }
    }

    public void sendFile(String source, String target) throws IgnisException{
        try {
            this.sem.acquire();
            Channel channel = session.openChannel("sftp");
            ChannelSftp sftp = (ChannelSftp) channel;
            sftp.put(source, target);
            channel.disconnect();
        } catch (JSchException | InterruptedException | SftpException ex) {
            throw new IgnisException("File could not be sent", ex);
        }finally {
            this.sem.release();
        }
    }

}
