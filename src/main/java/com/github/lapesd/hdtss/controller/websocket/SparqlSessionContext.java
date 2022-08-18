package com.github.lapesd.hdtss.controller.websocket;

import com.github.lapesd.hdtss.controller.execution.SparqlExecutor;
import io.micronaut.context.annotation.Property;
import jakarta.inject.Inject;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

@Slf4j
@Getter @Accessors(fluent = true)
public class SparqlSessionContext {
    private final @NonNull SparqlExecutor executor;
    private final int actionQueueCapacity, bindRequest, windowRows;
    private final long windowNanos, pingIntervalNanos;
    private final boolean tracing;

    @Inject
    public SparqlSessionContext(@NonNull SparqlExecutor executor,
                                @Property(name = "sparql.ws.action-queue", defaultValue = "8")
                                        int actionQueueCapacity,
                                @Property(name = "sparql.ws.bind-request", defaultValue = "64")
                                        int bindRequest,
                                @Property(name = "sparql.ws.window.rows", defaultValue = "16")
                                        int windowRows,
                                @Property(name = "sparql.ws.window.us", defaultValue = "500")
                                        long windowMicros,
                                @Property(name = "sparql.ws.ping.secs", defaultValue = "120")
                                        int pingIntervalSecs
    ) {
        this.executor = executor;
        this.actionQueueCapacity = actionQueueCapacity;
        this.bindRequest = bindRequest;
        this.windowRows = windowRows;
        this.windowNanos = windowMicros*1000L;
        this.pingIntervalNanos = pingIntervalSecs*1_000_000_000L;
        this.tracing = log.isTraceEnabled();
    }
}

