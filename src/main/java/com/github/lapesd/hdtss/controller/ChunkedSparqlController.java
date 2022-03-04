package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.SparqlParser;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import com.github.lapesd.hdtss.sparql.results.chunked.ChunkedEncoder;
import com.github.lapesd.hdtss.utils.QueryExecutionScheduler;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.*;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.*;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.reactivestreams.Publisher;
import reactor.core.scheduler.Scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;

@Controller(value = "/sparql",
            produces = {SparqlMediaTypes.RESULTS_TSV, SparqlMediaTypes.RESULTS_JSON,
                        SparqlMediaTypes.RESULTS_XML, SparqlMediaTypes.RESULTS_CSV,
                        MediaType.APPLICATION_JSON, MediaType.APPLICATION_XHTML,
                        MediaType.TEXT_HTML, MediaType.TEXT_PLAIN})
@Singleton
@Requires(property = "sparql.endpoint.flow", value = "CHUNKED", defaultValue = "CHUNKED")
@Slf4j
public class ChunkedSparqlController extends HeartBeatingSparqlController {
    private final @NonNull Scheduler scheduler;
    private final @NonNull SparqlParser parser;
    private final @NonNull OpExecutorDispatcher dispatcher;
    private final Map<@NonNull MediaType, ChunkedEncoder> type2encoder;
    private final @NonNull SparqlErrorHandler errorHandler;
    private final @NonNull AtomicLong nextQueryId = new AtomicLong(1);

    public static final class NoEncoderException extends RuntimeException {}

    @Inject
    public ChunkedSparqlController(@Named(QueryExecutionScheduler.NAME) @NonNull Scheduler scheduler,
                                   @NonNull SparqlParser parser,
                                   @NonNull OpExecutorDispatcher dispatcher,
                                   @NonNull SparqlErrorHandler errorHandler,
                                   @NonNull List<@NonNull ChunkedEncoder> encoders) {
        this.scheduler = scheduler;
        this.parser = parser;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.type2encoder = new HashMap<>();
        for (var it = encoders.listIterator(encoders.size()); it.hasPrevious(); ) {
            ChunkedEncoder encoder = it.previous();
            for (MediaType type : encoder.mediaTypes())
                type2encoder.put(type, encoder);
        }
    }

    @Override public @NonNull SparqlParser             parser() { return parser; }
    @Override public @NonNull OpExecutorDispatcher dispatcher() { return dispatcher; }

    private @NonNull Publisher<byte[]> answer(@Nullable String sparql, @Nullable String out,
                                              @Nullable String output, @NonNull HttpHeaders headers) {
        long start = nanoTime();
        sparql = sparql == null ? "" : sparql;
        String finalSparql = sparql;
        logQuery(sparql);
        MediaType mt = SparqlMediaTypes.firstResultType(headers.accept());
        if (mt == null)
            mt = SparqlMediaTypes.resultTypeFromShortName(out, output);
        if (mt == null)
            mt = SparqlMediaTypes.RESULTS_JSON_TYPE;
        ChunkedEncoder encoder = type2encoder.getOrDefault(mt, null);
        if (encoder == null)
            throw new NoEncoderException();
        long queryId = nextQueryId.getAndIncrement();
        log.debug("Processing query {}, mt={}, sparql={}", queryId, mt, sparql);
        return encoder.encode(mt, dispatcher.execute(parser.parse(sparql)))
                .subscribeOn(scheduler)
                .doOnComplete(() ->
                    log.debug("Query {}: completed serialization of all rows after {}ms",
                              queryId, MILLISECONDS.convert(nanoTime()-start, NANOSECONDS)))
                .doOnError(err -> {
                    String name = err == null ? "null" : err.getClass().getSimpleName();
                    log.error("Query {}: failed with {}, sparql={}",
                              queryId, name, finalSparql, err);
                });
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
