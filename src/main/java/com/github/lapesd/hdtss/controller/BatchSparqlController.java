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
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

@Controller(value = "/sparql",
            produces = {SparqlMediaTypes.RESULTS_TSV, SparqlMediaTypes.RESULTS_JSON,
                        SparqlMediaTypes.RESULTS_XML, SparqlMediaTypes.RESULTS_CSV})
@Singleton
@Slf4j
@Requires(property = "sparql.endpoint.flow", value = "BATCH")
public record BatchSparqlController(
        @NonNull SparqlParser parser,
        @NonNull OpExecutorDispatcher dispatcher,
        @NonNull SparqlErrorHandler errorHandler
) implements SparqlController {
    @Get
    @ExecuteOn(TaskExecutors.IO)
    public @NonNull QuerySolutions get(@NonNull String query) {
        return dispatcher.execute(parser.parse(query));
    }

    @Post(consumes = MediaType.APPLICATION_FORM_URLENCODED)
    @ExecuteOn(TaskExecutors.IO)
    public @NonNull QuerySolutions postForm(@NonNull String query) {
        return dispatcher.execute(parser.parse(query));
    }

    @Post(consumes = SparqlMediaTypes.QUERY)
    @ExecuteOn(TaskExecutors.IO)
    public @NonNull QuerySolutions post(@NonNull @Body String query) {
        return dispatcher.execute(parser.parse(query));
    }

    @Error
    @Produces(MediaType.TEXT_PLAIN)
    public HttpResponse<String> handle(@NonNull HttpRequest<?> request,
                                       @NonNull RuntimeException e) {
        return errorHandler.handle(request, e);
    }
}
