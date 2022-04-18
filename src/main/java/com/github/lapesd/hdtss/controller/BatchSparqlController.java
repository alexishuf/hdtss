package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.GetPredicatesExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.SparqlParser;
import com.github.lapesd.hdtss.sparql.optimizer.OptimizerRunner;
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
import org.slf4j.Logger;

import static java.lang.System.nanoTime;

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
    private final @NonNull GetPredicatesExecutor predicatesExecutor;
    private final @NonNull OptimizerRunner optimizer;

    @Inject public BatchSparqlController(@NonNull SparqlParser parser,
                                         @NonNull OpExecutorDispatcher dispatcher,
                                         @NonNull SparqlErrorHandler errorHandler,
                                         @NonNull GetPredicatesExecutor predicatesExecutor,
                                         @NonNull OptimizerRunner optimizer) {
        this.parser = parser;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.predicatesExecutor = predicatesExecutor;
        this.optimizer = optimizer;
    }

    @Override public @NonNull SparqlParser parser() { return parser; }
    @Override public @NonNull OpExecutorDispatcher dispatcher() { return dispatcher; }
    @Override protected @NonNull Logger log() { return log; }
    @Override protected boolean isSerializationTimed() { return false; }

    private @NonNull QuerySolutions answer(@NonNull String query) {
        //parse
        long start = nanoTime();
        QueryInfo.QueryInfoBuilder info = start(query);
        Op parsed = parser.parse(query);
        info.parseNs(nanoTime()-start);

        //try to dispatch predicates query
        long dispatchStart = nanoTime();
        QuerySolutions solutions = predicatesExecutor.tryExecute(parsed);
        if (solutions != null) {
            solutions = new BatchQuerySolutions(solutions);
            info.dispatchNs(nanoTime()-dispatchStart);
        } else {
            //optimize (and stop dispatch window)
            long optimizeStart = nanoTime();
            long dispatchNs = optimizeStart-dispatchStart;
            Op optimized = optimizer.optimize(parsed);
            info.optimizeNs(nanoTime()-optimizeStart);

            //dispatch optimized
            dispatchStart = nanoTime();
            solutions = new BatchQuerySolutions(dispatcher.execute(optimized));
            info.dispatchNs(dispatchNs + nanoTime()-dispatchStart);
        }
        log(info.totalNs(nanoTime()-start).rows(solutions.list().size()).build());
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
