package org.ignis.backend.allocator.ancoris.beans;

/**
 *
 * @author César Pomar
 */
public class EventsResponse extends EventsBase {

    public final static class OnExitResponse extends OnExit {

        public OnExitResponse() {
        }

        protected OnExitResponse setRestart(Boolean restart) {
            this.restart = restart;
            return this;
        }

        protected OnExitResponse setDestroy(Boolean destroy) {
            this.destroy = destroy;
            return this;
        }

    }

    public EventsResponse() {
    }

    @Override
    public OnExitResponse getOn_exit() {
        return (OnExitResponse) on_exit;
    }

    protected void setOn_exit(OnExitResponse on_exit) {
        this.on_exit = on_exit;
    }

}
