package com.github.lapesd.hdtss.controller.websocket;

import com.github.lapesd.hdtss.controller.execution.SparqlExecutor;
import com.github.lapesd.hdtss.sparql.optimizer.OptimizerRunner;
import com.github.lapesd.hdtss.sparql.optimizer.impl.AllOptimizerRunner;
import com.github.lapesd.hdtss.sparql.optimizer.impl.JoinOrderOptimizer;
import com.github.lapesd.hdtss.sparql.optimizer.impl.NoneOptimizerRunner;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.MediaType;
import io.micronaut.websocket.WebSocketSession;
import jakarta.inject.Inject;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.function.Consumer;

@Slf4j
@Getter @Accessors(fluent = true)
public class SparqlSessionContext {
    private final @NonNull SparqlExecutor executor;
    private final @NonNull OptimizerRunner bindOptimizer;
    private final int actionQueueCapacity, bindRequest, windowRows;
    private final long windowNanos;
    private final boolean tracing;


    @Inject
    public SparqlSessionContext(@NonNull SparqlExecutor executor,
                                @io.micronaut.core.annotation.Nullable
                                        JoinOrderOptimizer joinOrderOptimizer,
                                @Property(name = "sparql.ws.action-queue", defaultValue = "8")
                                        int actionQueueCapacity,
                                @Property(name = "sparql.ws.bind-request", defaultValue = "64")
                                        int bindRequest,
                                @Property(name = "sparql.ws.window.rows", defaultValue = "16")
                                        int windowRows,
                                @Property(name = "sparql.ws.window.us", defaultValue = "500")
                                        long windowMicros
    ) {
        this.executor = executor;
        this.bindOptimizer = joinOrderOptimizer == null
                ? new NoneOptimizerRunner()
                : new AllOptimizerRunner(List.of(joinOrderOptimizer));
        this.actionQueueCapacity = actionQueueCapacity;
        this.bindRequest = bindRequest;
        this.windowRows = windowRows;
        this.windowNanos = windowMicros*1000L;
        this.tracing = log.isTraceEnabled();
    }

    public boolean windowEnabled() { return windowNanos > 1000 && windowRows > 1; }

    public void sendSync(@NonNull WebSocketSession session,
                         @NonNull CharSequence msg) throws Throwable {
        if (tracing)
            log.trace("{} <<< {}", session.getId(), msg.toString().replace("\n", "\\n"));
        session.sendSync(msg, MediaType.TEXT_PLAIN_TYPE);
    }

    public void sendAsync(@NonNull WebSocketSession session, @NonNull CharSequence msg,
                          @Nullable Runnable onComplete, @Nullable Consumer<Throwable> onError) {
        if (tracing)
            log.trace("{} <<< {}", session.getId(), msg.toString().replace("\n", "\\n"));
        session.sendAsync(msg, MediaType.TEXT_PLAIN_TYPE).whenComplete((cs, err) -> {
            if (err != null) {
                if (onError == null)
                    log.error("{} ({}) sending {}.", err.getClass().getSimpleName(), err, msg);
                else
                    onError.accept(err);
            } else if (onComplete != null) {
                onComplete.run();
            }
        });
    }
}

