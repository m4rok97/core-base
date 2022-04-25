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

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author César Pomar
 */
public class ISSH {

    private final String privateKey;
    private final String publicKey;
    private final AtomicInteger localPort;
    private final int remotePortInit;

    private boolean portForwarding;

    public ISSH(int localPortinit, int remotePortInit, String privateKey, String publicKey) {
        this.localPort = new AtomicInteger(localPortinit);
        this.remotePortInit = remotePortInit;
        if (privateKey != null && publicKey != null) {
            this.privateKey = privateKey;
            this.publicKey = publicKey;
        } else {
            ByteArrayOutputStream privateKeyBuff = new ByteArrayOutputStream(2048);
            ByteArrayOutputStream publicKeyBuff = new ByteArrayOutputStream(2048);
            try {
                KeyPair keyPair = KeyPair.genKeyPair(new JSch(), KeyPair.RSA, 2048);
                keyPair.writePrivateKey(privateKeyBuff);
                keyPair.writePublicKey(publicKeyBuff, "");
            } catch (JSchException ex) {
            }
            this.privateKey = privateKeyBuff.toString();
            this.publicKey = publicKeyBuff.toString();
        }
    }

    public ISSH(int localPortinit, int remotePortInit) {
        this(localPortinit, remotePortInit, null, null);
    }

    public String getPublicKey() {
        return publicKey;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public void setPortForwarding(boolean portForwarding) {
        this.portForwarding = portForwarding;
    }

    public ITunnel createTunnel() {
        return new ITunnel(localPort, remotePortInit, portForwarding, privateKey, publicKey);
    }

}
