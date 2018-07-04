package org.ignis.backend.allocator.ancoris.beans;

import java.util.List;

/**
 *
 * @author César Pomar
 */
public abstract class TaskBase {

    protected String group;
    protected String image;
    protected ResourceBase resources;
    protected OptionsBase opts;
    protected EventsBase events;
    protected List<String> args;

    public TaskBase() {
    }

    public String getGroup() {
        return group;
    }

    public String getImage() {
        return image;
    }

    public ResourceBase getResources() {
        return resources;
    }

    public OptionsBase getOpts() {
        return opts;
    }

    public EventsBase getEvents() {
        return events;
    }

    public List<String> getArgs() {
        return args;
    }

}
