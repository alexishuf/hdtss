package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.controller.execution.SparqlExecutor;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import static java.lang.System.nanoTime;

@Controller(value = "/sparql",
            produces = {SparqlMediaTypes.RESULTS_TSV, SparqlMediaTypes.RESULTS_JSON,
                        SparqlMediaTypes.RESULTS_XML, SparqlMediaTypes.RESULTS_CSV})
@Singleton
@Slf4j
@Requires(property = "sparql.endpoint.flow", value = "BATCH")
@RequiredArgsConstructor
public class BatchSparqlController implements SparqlController {
    private final @NonNull SparqlExecutor executor;
    private final @NonNull SparqlErrorHandler errorHandler;

    private @NonNull QuerySolutions answer(@NonNull String query) {
        SparqlExecutor.DispatchResult dr = executor.dispatch(query);
        QuerySolutions solutions = dr.solutions();
        if (!(solutions instanceof BatchQuerySolutions)) {
            long reference = nanoTime();
            solutions = new BatchQuerySolutions(solutions);
            dr.info().addDispatchNs(nanoTime()-reference);
        }
        executor.notify(dr.info().rows(solutions.list().size()).build());
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
