package com.github.lapesd.hdtss.controller.websocket.task;

import com.github.lapesd.hdtss.controller.execution.QueryInfo;
import com.github.lapesd.hdtss.controller.execution.SparqlExecutor;
import com.github.lapesd.hdtss.controller.websocket.SparqlSessionContext;
import io.micronaut.websocket.WebSocketSession;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class QueryTask extends AbstractQueryTask {
    private final @NonNull String sparql;

    public QueryTask(@NonNull SparqlSessionContext context, @NonNull WebSocketSession session,
                     @NonNull TaskTerminationListener onTermination, @NonNull String sparql) {
        super(context, session, onTermination);
        this.sparql = sparql;
    }

    @Override protected QueryInfo.@Nullable Builder doStart() {
        SparqlExecutor.DispatchResult dr;
        try {
            dr = context.executor().dispatch(sparql);
        } catch (Throwable error) {
            serializer.end("!error "+error.toString().replace("\n", "\\n"), error);
            return null;
        }
        context.executor().scheduler().schedule(() -> work(dr));
        return dr.info();
    }


    private void work(SparqlExecutor.DispatchResult dr) {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName("QueryTask-worker-"+sessionId);
        try {
            sendHeaders(dr.solutions().varNames());
            if (serialize(dr.solutions()))
                sendEnd();
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }
}
