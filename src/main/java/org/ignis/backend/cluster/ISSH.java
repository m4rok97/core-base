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
import org.ignis.backend.scheduler.IScheduler;

/**
 *
 * @author César Pomar
 */
public class ISSH {

    private final String privateKey;
    private final String publicKey;
    private final AtomicInteger localPort;
    private final int remotePortInit;

    public ISSH(int localPortinit, int remotePortInit) {
        localPort = new AtomicInteger(localPortinit);
        this.remotePortInit = remotePortInit;
        ByteArrayOutputStream privateKeyBuff = new ByteArrayOutputStream(2048);
        ByteArrayOutputStream publicKeyBuff = new ByteArrayOutputStream(2048);
        try {
            KeyPair keyPair = KeyPair.genKeyPair(new JSch(), KeyPair.RSA, 2048);
            keyPair.writePrivateKey(privateKeyBuff);
            keyPair.writePublicKey(publicKeyBuff, "");
        } catch (JSchException ex) {
        }
        privateKey = privateKeyBuff.toString();
        publicKey = publicKeyBuff.toString();

    }

    public String getPublicKey() {
        return publicKey;
    }

    public ITunnel createTunnel(IScheduler scheduler) {
        return new ITunnel(scheduler, localPort, remotePortInit, privateKey);
    }

}
