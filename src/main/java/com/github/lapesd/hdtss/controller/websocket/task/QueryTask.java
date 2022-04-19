package com.github.lapesd.hdtss.controller.websocket.task;

import com.github.lapesd.hdtss.controller.execution.QueryInfo;
import com.github.lapesd.hdtss.controller.execution.SparqlExecutor;
import com.github.lapesd.hdtss.controller.websocket.SparqlSessionContext;
import io.micronaut.websocket.WebSocketSession;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

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
            terminate(error);
            return null;
        }
        context.executor().scheduler().schedule(() -> {
            SyncSender sender = new SyncSender();
            sendHeaders(sender, dr.solutions().varNames());
            if (serialize(sender, dr.solutions()))
                terminate(null);
        });
        return dr.info();
    }
}
