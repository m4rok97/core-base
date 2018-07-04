package org.ignis.backend.allocator.ancoris.beans;

/**
 *
 * @author César Pomar
 */
public final class TaskResponse extends TaskBase {

    private String id;
    private String host;
    private String node;
    private StatusResponse statusResponse;
    private Double completion;

    public TaskResponse() {
        statusResponse = new StatusResponse();
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public String getNode() {
        return node;
    }

    public StatusResponse getStatusResponse() {
        return statusResponse;
    }

    public Double getCompletion() {
        return completion;
    }

    protected String getStatus() {
        return statusResponse.getStatus();
    }

    protected void setId(String id) {
        this.id = id;
    }

    protected void setHost(String host) {
        this.host = host;
    }

    protected void setNode(String node) {
        this.node = node;
    }

    protected void setStatus(String status) {
        statusResponse.setStatus(status);
    }

    protected void setCompletion(Double completion) {
        this.completion = completion;
    }

    protected void setGroup(String group) {
        this.group = group;
    }

    protected void setImage(String image) {
        this.image = image;
    }

    protected void setResources(ResourceResponse resources) {
        this.resources = resources;
    }

    protected void setOpts(OptionsResponse opts) {
        this.opts = opts;
    }

    protected void setEvents(EventsResponse events) {
        this.events = events;
    }

    @Override
    public ResourceResponse getResources() {
        return (ResourceResponse) resources;
    }

    @Override
    public EventsResponse getEvents() {
        return (EventsResponse) events;
    }

    @Override
    public OptionsResponse getOpts() {
        return (OptionsResponse) opts;
    }

}
