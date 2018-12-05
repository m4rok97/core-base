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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.ignis.backend.allocator.IContainerStub;
import org.ignis.backend.exception.IgnisException;
import org.ignis.backend.properties.IProperties;
import org.ignis.backend.properties.IPropsKeys;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author César Pomar
 */
@SuppressWarnings("unchecked")
public final class IAncorisContainerStub extends IContainerStub {

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private final String url;
    private final String group;
    private final OkHttpClient client;
    private final JSONObject requestJSON;
    private JSONObject responseJSON;

    public IAncorisContainerStub(IProperties properties, String url, OkHttpClient client) throws IgnisException {
        super(properties);
        this.url = url;
        this.client = client;
        this.requestJSON = new JSONObject();
        group = System.getenv("GROUP_ID");
        if (group == null) {
            throw new IgnisException("GROUP_ID not exists, aborting");
        }
        parseRequest();
    }

    private void parseRequest() throws IgnisException {
        requestJSON.put("image", properties.getProperty(IPropsKeys.EXECUTOR_IMAGE));
        requestJSON.put("group", group);
        //RESOURCES
        JSONObject resources = new JSONObject();
        resources.put("cores", properties.getInteger(IPropsKeys.EXECUTOR_CORES));
        resources.put("memory", properties.getString(IPropsKeys.EXECUTOR_MEMORY));
        resources.put("swap", properties.getString(IPropsKeys.EXECUTOR_SWAP));
        JSONArray volumes = new JSONArray();
        //  VOLUMES
        JSONObject dfs = new JSONObject();
        dfs.put("id", properties.getString(IPropsKeys.DFS_ID));
        dfs.put("mode", "rw");
        dfs.put("path", properties.getString(IPropsKeys.DFS_HOME));
        volumes.add(dfs);
        resources.put("volumes", volumes);
        //  DEVICES
        resources.put("devices", new JSONArray());
        //  PORTS
        JSONArray ports = new JSONArray();
        ports.add(properties.getInteger(IPropsKeys.TRANSPORT_PORT));
        ports.add(properties.getInteger(IPropsKeys.MANAGER_RPC_PORT));
        resources.put("ports", ports);
        requestJSON.put("resources", resources);
        //OPTIONS
        JSONObject opts = new JSONObject();
        opts.put("swappiness", properties.getString(IPropsKeys.EXECUTOR_SWAPPINESS));
        requestJSON.put("opts", opts);
        //ENVIRONMENT
        JSONObject environment = new JSONObject();
        environment.put("IGNIS_HOME", properties.getString(IPropsKeys.HOME));
        requestJSON.put("environment", environment);
        //EVENTS
        JSONObject events = new JSONObject();
        JSONObject on_exit = new JSONObject();
        on_exit.put("restart", false);
        on_exit.put("destroy", true);
        events.put("on_exit", on_exit);
        requestJSON.put("events", events);
        //ARGUMENTS
        JSONArray args = new JSONArray();
        args.add("ignis-manager");
        args.add(String.valueOf(properties.getInteger(IPropsKeys.MANAGER_RPC_PORT)));
        args.add(String.valueOf(properties.getInteger(IPropsKeys.MANAGER_EXECUTORS_PORT)));
        args.add(String.valueOf(properties.getInteger(IPropsKeys.MANAGER_RPC_COMPRESSION)));
        requestJSON.put("args", args);

    }

    private String getId() {
        if (isRunning()) {
            return (String) responseJSON.get("id");
        }
        return null;
    }

    @Override
    public boolean isRunning() {
        return responseJSON != null;
    }

    @Override
    public String getHost() {
        if (isRunning()) {
            return (String) responseJSON.get("host");
        }
        return null;
    }

    @Override
    public void request() throws IgnisException {
        RequestBody body = RequestBody.create(JSON, requestJSON.toJSONString());
        Request request = new Request.Builder()
                .url(url + "/tasks")
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                JSONParser parser = new JSONParser();
                try {
                    responseJSON = (JSONObject) parser.parse(response.body().charStream());
                } catch (ParseException ex) {
                    throw new IgnisException(ex.getMessage() + "\n" + response.body(), ex);
                }
            } else {
                throw new IgnisException(response.message() + "\n" + requestJSON.toJSONString());
            }
        } catch (IOException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    @Override
    public void destroy() throws IgnisException {
        Request request = new Request.Builder()
                .url(url + "/tasks/" + getId())
                .delete()
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException(response.message());
            }
        } catch (IOException ex) {
            throw new IgnisException(ex.getMessage(), ex);
        }
    }

    @Override
    public Map<Integer, Integer> getPorts() {
        if (isRunning()) {
            Map ports = new HashMap<>();
            ((JSONArray) ((JSONObject) responseJSON.get("resources")).get("ports")).forEach(p -> {
                ports.put(
                        ((Number) ((JSONObject) p).get("container")).intValue(),
                        ((Number) ((JSONObject) p).get("host")).intValue()
                );
            });
            return (Map<Integer, Integer>) ports;
        } else {
            return Collections.EMPTY_MAP;
        }
    }

}
