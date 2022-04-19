package com.github.lapesd.hdtss.controller.websocket;

import org.checkerframework.checker.nullness.qual.NonNull;

public sealed interface Action {
    record Query(@NonNull String sparql) implements Action {}
    record Bind(@NonNull String sparql) implements Action {}
    record Cancel() implements Action {}
    record QueueCap() implements Action {}

    Cancel CANCEL = new Cancel();
    QueueCap QUEUE_CAP = new QueueCap();
}
