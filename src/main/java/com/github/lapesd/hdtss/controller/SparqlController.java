package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.controller.execution.SparqlExecutor;
import com.github.lapesd.hdtss.controller.websocket.SparqlSession;
import com.github.lapesd.hdtss.controller.websocket.SparqlSessionContext;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.EmptySparqlException;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import com.github.lapesd.hdtss.sparql.results.chunked.ChunkedEncoder;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.*;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.*;
import io.micronaut.http.codec.MediaTypeCodec;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.websocket.CloseReason;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.*;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.System.nanoTime;
import static java.util.stream.Collectors.joining;

@Controller(value = "/sparql",
            produces = {SparqlMediaTypes.RESULTS_TSV, SparqlMediaTypes.RESULTS_JSON,
                        SparqlMediaTypes.RESULTS_XML, SparqlMediaTypes.RESULTS_CSV,
                        MediaType.APPLICATION_JSON,   MediaType.APPLICATION_XHTML,
                        MediaType.TEXT_HTML,          MediaType.TEXT_PLAIN})
@ServerWebSocket("/sparql/ws")
@Singleton
@Slf4j
public class SparqlController {
    private final @NonNull EndpointFlowType flowType;
    private final @NonNull SparqlExecutor executor;
    private final Map<@NonNull MediaType, ChunkedEncoder> type2encoder;
    private final Map<@NonNull MediaType, MediaTypeCodec> type2codec;
    private final @NonNull SparqlErrorHandler errorHandler;
    private final SparqlSessionContext ssContext;
    private final Map<String, SparqlSession> ssMap = new ConcurrentHashMap<>();
    private final boolean debug = log.isDebugEnabled();

    public static final class NoEncoderException extends RuntimeException {}

    public SparqlController(@Property(name = "sparql.endpoint.flow", defaultValue = "CHUNKED")
                                @NonNull EndpointFlowType flowType,
                            @NonNull SparqlExecutor sparqlExecutor,
                            @NonNull SparqlErrorHandler errorHandler,
                            @NonNull List<@NonNull ChunkedEncoder> encoders,
                            @NonNull List<@NonNull MediaTypeCodec> codecs,
                            @NonNull SparqlSessionContext ssContext) {
        this.flowType = flowType;
        this.executor = sparqlExecutor;
        this.errorHandler = errorHandler;
        this.type2encoder = new HashMap<>();
        this.type2codec = new HashMap<>();
        this.ssContext = ssContext;
        for (var it = encoders.listIterator(encoders.size()); it.hasPrevious(); ) {
            ChunkedEncoder encoder = it.previous();
            for (MediaType type : encoder.mediaTypes())
                type2encoder.put(type, encoder);
        }
        for (MediaType type : SparqlMediaTypes.RESULT_TYPES) {
            MediaTypeCodec selected = null;
            for (var it = codecs.iterator(); it.hasNext() && selected == null; ) {
                MediaTypeCodec codec = it.next();
                if (!codec.supportsType(QuerySolutions.class)) continue;
                if (codec.getMediaTypes().stream().noneMatch(type::matches)) continue;
                selected = codec;
            }
            if (selected == null)
                log.info("No encoding support for {}", type);
            else
                type2codec.put(type, selected);
        }
    }

    private @NonNull MediaType selectMT(@NonNull HttpRequest<?> request,
                                        @Nullable String... formats) {
        MediaType mt = SparqlMediaTypes.firstResultType(request.getHeaders().accept());
        if (mt == null) {
            HttpParameters params = request.getParameters();
            int start = 0;
            formats = formats == null ? new String[2]
                    : Arrays.copyOf(formats, 2+(start = formats.length));
            formats[start  ] = params.get("out",    String.class).orElse(null);
            formats[start+1] = params.get("output", String.class).orElse(null);
            mt = SparqlMediaTypes.resultTypeFromShortName(formats);
            if (mt == null)
                mt = SparqlMediaTypes.RESULTS_JSON_TYPE;
        }
        return mt;
    }

    private @NonNull HttpResponse<Publisher<byte[]>>
    answer(@Nullable String sparql, @NonNull MediaType mt) {
        if (sparql == null || sparql.isEmpty())
            throw new EmptySparqlException();
        MutableHttpResponse<Publisher<byte[]>> response;
        switch (flowType) {
            case CHUNKED -> {
                ChunkedEncoder encoder = type2encoder.getOrDefault(mt, null);
                if (encoder == null)
                    throw new NoEncoderException();
                response = HttpResponse.ok(executor.execute(sparql, encoder, mt));
            }
            case BATCH -> {
                SparqlExecutor.DispatchResult dr = executor.dispatch(sparql);
                QuerySolutions solutions = dr.solutions();
                if (!(solutions instanceof BatchQuerySolutions)) {
                    long reference = nanoTime();
                    solutions = new BatchQuerySolutions(solutions);
                    dr.info().addDispatchNs(nanoTime()-reference);
                }
                executor.notify(dr.info().rows(solutions.list().size()).build());
                MediaTypeCodec codec = type2codec.getOrDefault(mt, null);
                if (codec == null)
                    throw new NoEncoderException();
                response = HttpResponse.ok(Mono.just(codec.encode(solutions)));
            }
            default -> throw new UnsupportedOperationException("flowType="+flowType+" unknown");
        }
        return response.contentType(mt);
    }

    /* --- --- --- HTTP methods --- --- --- */

    @Get @ExecuteOn(TaskExecutors.IO)
    public @NonNull HttpResponse<Publisher<byte[]>> get(@NonNull HttpRequest<?> request) {
        String sparql = request.getParameters().get("query", String.class, "");
        return answer(sparql, selectMT(request));
    }

    @Post(consumes = MediaType.APPLICATION_FORM_URLENCODED) @ExecuteOn(TaskExecutors.IO)
    public @NonNull HttpResponse<Publisher<byte[]>>
    postForm(@NonNull String query, @Nullable String out, @Nullable String output,
             @NonNull HttpRequest<?> request) {
        return answer(query, selectMT(request, out, output));
    }

    @Post(consumes = SparqlMediaTypes.QUERY) @ExecuteOn(TaskExecutors.IO)
    public @NonNull HttpResponse<Publisher<byte[]>> post(HttpRequest<String> request) {
        return answer(request.getBody(String.class).orElse(null), selectMT(request));
    }

    /* --- --- --- websocket events --- --- --- */

    @OnOpen public void onOpen(@NonNull WebSocketSession session) {
        ssMap.put(session.getId(), new SparqlSession(ssContext, session));
    }

    @OnMessage public void onMessage(@NonNull String msg, @NonNull WebSocketSession session) {
        SparqlSession ss = ssMap.get(session.getId());
        if (ss == null)
            ssMap.put(session.getId(), ss = new SparqlSession(ssContext, session));
        ss.receive(msg);
    }

    @OnError public void onError(@NonNull WebSocketSession session, Throwable error) {
        String id = session.getId();
        SparqlSession ss = ssMap.get(id);
        if (debug)
            log.info("onError({}, {})", id, error, error);
        else
            log.info("onError({}, {})", id, Objects.toString(error));
        if (ss != null)
            close(ss, error, null);
        else if (session.isOpen())
            session.close();
    }

    @OnClose public void onClose(@NonNull WebSocketSession session, CloseReason reason) {
        String id = session.getId();
        SparqlSession ss = ssMap.get(id);
        if (ss == null) {
            log.debug("onClose({}, {}): untracked session", id, reason);
        } else {
            close(ss, null, reason);
        }
    }

    private void close(SparqlSession ss, @Nullable Throwable error, @Nullable CloseReason reason) {
        String id = ss.id();
        //noinspection resource
        ssMap.remove(id);
        try {
            Object why = error == null ? reason : error.getClass().getSimpleName();
            log.trace("Closing {} due to {}", ss, why);
            ss.close();
        } catch (Throwable t) {
            log.warn("Error closing {}: {}", ss, t, t);
        }
    }

    /* --- --- --- error handlers --- --- --- */

    @Error @Produces(MediaType.TEXT_PLAIN)
    public @NonNull HttpResponse<String> handle(@NonNull HttpRequest<?> request,
                                                @NonNull NoEncoderException ignored) {
        var msg = "Cannot produce query results in any of the requested media types: " +
                request.accept().stream().map(MediaType::getName).collect(joining(", "));
        return HttpResponse.status(HttpStatus.NOT_ACCEPTABLE)
                .contentType(MediaType.TEXT_PLAIN_TYPE).body(msg);
    }

    @Error @Produces(MediaType.TEXT_PLAIN)
    public HttpResponse<String> handle(@NonNull HttpRequest<?> request,
                                       @NonNull RuntimeException e) {
        return errorHandler.handle(request, e);
    }
}
