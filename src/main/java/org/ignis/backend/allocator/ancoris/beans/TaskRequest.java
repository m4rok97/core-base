package org.ignis.backend.allocator.ancoris.beans;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author César Pomar
 */
public final class TaskRequest extends TaskBase {

    public TaskRequest() {
        args = new ArrayList<>();
        resources = new ResourceRequest();
        events = new EventsRequest();
        opts = new OptionsRequest();
    }

    public TaskRequest setGroup(String group) {
        this.group = group;
        return this;
    }

    public TaskRequest setImage(String image) {
        this.image = image;
        return this;
    }

    public TaskRequest setResources(ResourceRequest resources) {
        if (resources != null) {
            this.resources = resources;
        }
        return this;
    }

    public TaskRequest setOpts(OptionsBase opts) {
        if (opts != null) {
            this.opts = opts;
        }
        return this;
    }

    public TaskRequest setEvents(EventsBase events) {
        if (events != null) {
            this.events = events;
        }
        return this;
    }

    public TaskRequest setArgs(List<String> args) {
        if (args != null) {
            this.args = args;
        }
        return this;
    }

    @Override
    public ResourceRequest getResources() {
        return (ResourceRequest) resources;
    }

    @Override
    public EventsRequest getEvents() {
        return (EventsRequest) events;
    }

    @Override
    public OptionsRequest getOpts() {
        return (OptionsRequest) opts;
    }

}
