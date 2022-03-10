package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.GetPredicatesExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.SparqlParser;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.*;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Controller(value = "/sparql",
            produces = {SparqlMediaTypes.RESULTS_TSV, SparqlMediaTypes.RESULTS_JSON,
                        SparqlMediaTypes.RESULTS_XML, SparqlMediaTypes.RESULTS_CSV})
@Singleton
@Slf4j
@Requires(property = "sparql.endpoint.flow", value = "BATCH")
public class BatchSparqlController extends HeartBeatingSparqlController {
    private final @NonNull SparqlParser parser;
    private final @NonNull OpExecutorDispatcher dispatcher;
    private final @NonNull SparqlErrorHandler errorHandler;
    private final @NonNull AtomicLong nextQueryId = new AtomicLong(1);
    private final @NonNull GetPredicatesExecutor predicatesExecutor;

    @Inject public BatchSparqlController(@NonNull SparqlParser parser,
                                         @NonNull OpExecutorDispatcher dispatcher,
                                         @NonNull SparqlErrorHandler errorHandler,
                                         @NonNull GetPredicatesExecutor predicatesExecutor) {
        this.parser = parser;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.predicatesExecutor = predicatesExecutor;
    }

    @Override public @NonNull SparqlParser parser() { return parser; }
    @Override public @NonNull OpExecutorDispatcher dispatcher() { return dispatcher; }

    private @NonNull QuerySolutions answer(@NonNull String query) {
        logQuery(query);
        long queryId = nextQueryId.getAndIncrement();
        long start = nanoTime();
        log.debug("Starting query {}, sparql={}", queryId, query);
        Op parsed = parser.parse(query);
        QuerySolutions solutions = predicatesExecutor.tryExecute(parsed);
        if (solutions == null)
            solutions = new BatchQuerySolutions(dispatcher.execute(parsed));
        log.debug("Completed query {} after {}ms (serialization not started yet)",
                  queryId, MILLISECONDS.convert(nanoTime()-start, NANOSECONDS));
        return solutions;
    }

    @Get
    @ExecuteOn(TaskExecutors.IO)
    public @NonNull QuerySolutions get(@NonNull String query) {
        return answer(query);
    }

    @Post(consumes = MediaType.APPLICATION_FORM_URLENCODED)
    @ExecuteOn(TaskExecutors.IO)
    public @NonNull QuerySolutions postForm(@NonNull String query) {
        return answer(query);
    }

    @Post(consumes = SparqlMediaTypes.QUERY)
    @ExecuteOn(TaskExecutors.IO)
    public @NonNull QuerySolutions post(@NonNull @Body String query) {
        return answer(query);
    }

    @Error
    @Produces(MediaType.TEXT_PLAIN)
    public HttpResponse<String> handle(@NonNull HttpRequest<?> request,
                                       @NonNull RuntimeException e) {
        return errorHandler.handle(request, e);
    }
}
