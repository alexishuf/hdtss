package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
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

    @Inject public BatchSparqlController(@NonNull SparqlParser parser,
                                         @NonNull OpExecutorDispatcher dispatcher,
                                         @NonNull SparqlErrorHandler errorHandler) {
        this.parser = parser;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
    }

    @Override public @NonNull SparqlParser parser() { return parser; }
    @Override public @NonNull OpExecutorDispatcher dispatcher() { return dispatcher; }

    private @NonNull QuerySolutions answer(@NonNull String query) {
        logQuery(query);
        return dispatcher.execute(parser.parse(query));
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
