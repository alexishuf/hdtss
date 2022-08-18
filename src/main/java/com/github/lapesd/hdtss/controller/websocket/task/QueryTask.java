package com.github.lapesd.hdtss.controller.websocket.task;

import com.github.lapesd.hdtss.controller.execution.QueryInfo;
import com.github.lapesd.hdtss.controller.execution.SparqlExecutor;
import com.github.lapesd.hdtss.controller.websocket.SparqlSession;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class QueryTask extends AbstractQueryTask {
    private final @NonNull String sparql;

    public QueryTask(@NonNull SparqlSession session, @NonNull TaskTerminationListener onTermination,
                     @NonNull String sparql) {
        super(session, onTermination);
        this.sparql = sparql;
    }

    @Override protected QueryInfo.@Nullable Builder doStart() {
        SparqlExecutor.DispatchResult dr;
        SparqlExecutor executor = session.context().executor();
        try {
            dr = executor.dispatch(sparql);
        } catch (Throwable error) {
            serializer.end("!error "+error.toString().replace("\n", "\\n"), error);
            return null;
        }
        executor.scheduler().schedule(() -> work(dr));
        return dr.info();
    }


    private void work(SparqlExecutor.DispatchResult dr) {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName("QueryTask-worker-"+session.id());
        try {
            sendHeaders(dr.solutions().varNames());
            if (serialize(dr.solutions()))
                sendEnd();
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }
}
