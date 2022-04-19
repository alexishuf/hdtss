package com.github.lapesd.hdtss.controller.websocket.task;

@FunctionalInterface
public interface TaskTerminationListener {
    enum Cause {
        COMPLETION,
        CANCEL,
        FAILED
    }

    void onTerminate(Cause cause);
}
