package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.GetPredicatesExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.SparqlParser;
import com.github.lapesd.hdtss.sparql.optimizer.OptimizerRunner;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import com.github.lapesd.hdtss.sparql.results.chunked.ChunkedEncoder;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.*;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.System.nanoTime;
import static java.util.stream.Collectors.joining;

@Controller(value = "/sparql",
            produces = {SparqlMediaTypes.RESULTS_TSV, SparqlMediaTypes.RESULTS_JSON,
                        SparqlMediaTypes.RESULTS_XML, SparqlMediaTypes.RESULTS_CSV,
                        MediaType.APPLICATION_JSON, MediaType.APPLICATION_XHTML,
                        MediaType.TEXT_HTML, MediaType.TEXT_PLAIN})
@Singleton
@Requires(property = "sparql.endpoint.flow", value = "CHUNKED", defaultValue = "CHUNKED")
@Slf4j
@Accessors(fluent = true)
public class ChunkedSparqlController extends HeartBeatingSparqlController {
    @Getter private final @NonNull SparqlParser parser;
    @Getter private final @NonNull OpExecutorDispatcher dispatcher;
    private final Map<@NonNull MediaType, ChunkedEncoder> type2encoder;
    private final @NonNull SparqlErrorHandler errorHandler;
    @Getter private final @NonNull GetPredicatesExecutor predicatesExecutor;
    private final @NonNull OptimizerRunner optimizer;

    public static final class NoEncoderException extends RuntimeException {}

    @Inject
    public ChunkedSparqlController(@NonNull SparqlParser parser,
                                   @NonNull OpExecutorDispatcher dispatcher,
                                   @NonNull SparqlErrorHandler errorHandler,
                                   @NonNull List<@NonNull ChunkedEncoder> encoders,
                                   @NonNull GetPredicatesExecutor predicatesExecutor,
                                   @NonNull OptimizerRunner optimizer) {
        this.parser = parser;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.predicatesExecutor = predicatesExecutor;
        this.optimizer = optimizer;
        this.type2encoder = new HashMap<>();
        for (var it = encoders.listIterator(encoders.size()); it.hasPrevious(); ) {
            ChunkedEncoder encoder = it.previous();
            for (MediaType type : encoder.mediaTypes())
                type2encoder.put(type, encoder);
        }
    }

    @Override protected @NonNull Logger log() { return log; }
    @Override protected boolean isSerializationTimed() { return true; }

    private @NonNull Publisher<byte[]> answer(@Nullable String sparql, @Nullable String out,
                                              @Nullable String output, @NonNull HttpHeaders headers) {
        long start = nanoTime();
        sparql = sparql == null ? "" : sparql;
        QueryInfo.QueryInfoBuilder info = start(sparql);
        try {
            Op parsed = parser.parse(sparql);
            info.parseNs(nanoTime() - start);

            // dispatch init and dispatch predicate queries
            long dispatchStart = nanoTime();
            MediaType mt = chooseMediaType(out, output, headers);
            ChunkedEncoder encoder = type2encoder.getOrDefault(mt, null);
            if (encoder == null)
                throw new NoEncoderException();
            QuerySolutions solutions = predicatesExecutor.tryExecute(parsed);
            long dispatchNs = nanoTime() - dispatchStart;

            if (solutions == null) {
                //optimize
                long optimizeStart = nanoTime();
                Op optimized = optimizer.optimize(parsed);
                info.optimizeNs(nanoTime() - optimizeStart);

                //dispatch optimized
                dispatchStart = nanoTime();
                solutions = dispatcher.execute(optimized);
                dispatchNs += System.nanoTime() - dispatchStart;
            }
            info.dispatchNs(dispatchNs);

            //execution
            return encoder.encode(mt, solutions).onTermination((err, cancelled, rows, items, nanos)
                    -> log(info.error(err).cancelled(cancelled).rows(rows)
                               .totalNs(nanoTime() - start).build()));
        } catch (Throwable t) {
            log(info.error(t).rows(0).totalNs(nanoTime()-start).build());
            throw t;
        }
    }

    @NonNull
    private MediaType chooseMediaType(String out, String output, @NonNull HttpHeaders headers) {
        MediaType mt = SparqlMediaTypes.firstResultType(headers.accept());
        if (mt == null)
            mt = SparqlMediaTypes.resultTypeFromShortName(out, output);
        if (mt == null)
            mt = SparqlMediaTypes.RESULTS_JSON_TYPE;
        return mt;
    }

    @Get
    public @NonNull Publisher<byte[]> get(@Nullable String query, @Nullable String out,
                                          @Nullable String output, @NonNull HttpHeaders headers) {
        return answer(query, out, output, headers);
    }

    @Post(consumes = MediaType.APPLICATION_FORM_URLENCODED)
    public @NonNull Publisher<byte[]> postForm(@Nullable String query, @Nullable String out,
                                          @Nullable String output, @NonNull HttpHeaders headers) {
        return answer(query, out, output, headers);
    }

    @Post(consumes = SparqlMediaTypes.QUERY)
    public @NonNull Publisher<byte[]> post(@NonNull @Body String query, @Nullable String out,
                                      @Nullable String output, @NonNull HttpHeaders headers) {
        return answer(query, out, output, headers);
    }

    @Error @Produces(MediaType.TEXT_PLAIN)
    public @NonNull HttpResponse<String> handle(@NonNull HttpRequest<?> request,
                                                @NonNull NoEncoderException ignored) {
        var msg = "Cannot produce query results in any of the requested media types: " +
                  request.accept().stream().map(MediaType::getName).collect(joining(", "));
        return HttpResponse.status(HttpStatus.NOT_ACCEPTABLE)
                .contentType(MediaType.TEXT_PLAIN_TYPE).body(msg);
    }

    @Error @Produces(MediaType.TEXT_PLAIN)
    public @NonNull HttpResponse<String> handle(@NonNull HttpRequest<?> request,
                                                @NonNull RuntimeException e) {
        return errorHandler.handle(request, e);
    }
}
