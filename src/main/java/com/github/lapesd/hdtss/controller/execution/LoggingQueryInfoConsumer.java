package com.github.lapesd.hdtss.controller.execution;

import com.github.lapesd.hdtss.utils.LogUtils;
import io.micronaut.context.annotation.Property;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

import static java.util.Objects.requireNonNull;

@Slf4j
public class LoggingQueryInfoConsumer implements QueryInfoConsumer {
    boolean logQueries;

    public LoggingQueryInfoConsumer(@Property(name = "sparql.log.query", defaultValue = "false")
                                                boolean logQueries) {
        this.logQueries = logQueries;
    }

    @Override public void accept(@NonNull QueryInfo info) {
        if (info.error() != null) {
            log.error("Query {}: failed with {} after {}ms and {} rows, parse={}ms, " +
                            "dispatch={}ms, optimize={}ms, sparql=\n{}", info.id(),
                      requireNonNull(info.error()).getClass().getSimpleName(), info.totalMs(),
                      info.rows(), info.parseMs(), info.dispatchMs(), info.optimizeMs(),
                      info.indentedSparql(4));
        } else {
            String fmt = "Query {}: {} {} rows in {}ms, parse={}ms, dispatch={}ms, optimize={}ms, sparql=\n{}";
            LogUtils.log(log, logQueries ? Level.INFO : Level.DEBUG, fmt,
                         info.id(), info.cancelled() ? "cancelled after" : "completed with",
                         info.rows(), info.totalMs(), info.parseMs(), info.dispatchMs(),
                         info.optimizeMs(), info.indentedSparql(4));
        }
    }
}
