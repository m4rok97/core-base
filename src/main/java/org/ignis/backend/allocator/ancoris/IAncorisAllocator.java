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
package org.ignis.backend.allocator.ancoris;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.ignis.backend.allocator.IAllocator;
import org.ignis.backend.allocator.IContainerStub;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;

/**
 *
 * @author César Pomar
 */
public class IAncorisAllocator implements IAllocator {

    private final String url;
    private final OkHttpClient client;

    public IAncorisAllocator(String url) {
        this.url = url;
        this.client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    @Override
    public void ping() throws IgnisException {
        Request request = new Request.Builder()
                .url(url + "/status")
                .get()
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IgnisException(response.message());
            }
        } catch (IOException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    @Override
    public String getName() {
        return "ancoris";
    }

    @Override
    public IContainerStub getContainer(IProperties prop) throws IgnisException {
        return new IAncorisContainerStub(prop, url, client);
    }

}
