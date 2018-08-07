package org.ignis.backend.allocator.ancoris.beans;

/**
 *
 * @author César Pomar
 */
public abstract class EventsBase {

    public static abstract class OnExit {

        protected Boolean restart;
        protected Boolean destroy;

        public OnExit() {
        }

        public Boolean getRestart() {
            return restart;
        }

        public Boolean getDestroy() {
            return destroy;
        }

    }

    protected OnExit on_exit;

    public EventsBase() {
    }

    public OnExit getOn_exit() {
        return on_exit;
    }

}
