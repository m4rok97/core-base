package org.ignis.backend.allocator.ancoris.beans;

/**
 *
 * @author César Pomar
 */
public final class EventsRequest extends EventsBase {

    public final static class OnExitRequest extends OnExit {

        public OnExitRequest() {
        }

        public OnExitRequest setRestart(Boolean restart) {
            this.restart = restart;
            return this;
        }

        public OnExitRequest setDestroy(Boolean destroy) {
            this.destroy = destroy;
            return this;
        }

    }

    public EventsRequest() {
        on_exit = new OnExitRequest();
    }

    public EventsRequest setOnExit(OnExitRequest on_exit) {
        if (on_exit != null) {
            this.on_exit = on_exit;
        }
        return this;
    }

    public OnExitRequest getOn_exit() {
        return (OnExitRequest) on_exit;
    }

}
