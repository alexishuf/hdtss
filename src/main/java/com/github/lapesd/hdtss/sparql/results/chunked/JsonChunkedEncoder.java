package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import com.github.lapesd.hdtss.sparql.results.codecs.JSONCodec;
import com.github.lapesd.hdtss.utils.ByteArrayWriter;
import io.micronaut.http.MediaType;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;

@Singleton
public class JsonChunkedEncoder implements ChunkedEncoder{
    private static final @NonNull List<MediaType> mediaTypes =
            List.of(SparqlMediaTypes.RESULTS_JSON_TYPE);
    private static final byte @NonNull [] ASK_PROLOGUE =
            "{\"head\":{\"vars\":[]},\"boolean\":".getBytes(UTF_8);
    private static final byte @NonNull [] ASK_TRUE = "true}".getBytes(UTF_8);
    private static final byte @NonNull [] ASK_FALSE = "false}".getBytes(UTF_8);
    private static final byte @NonNull [] ROWS_EPILOGUE = "]}}".getBytes(UTF_8);

    @Override public @NonNull List<@NonNull MediaType> mediaTypes() {
        return mediaTypes;
    }

    @Override
    public @NonNull Flux<byte[]> encode(@NonNull MediaType mediaType,
                                        @NonNull QuerySolutions solutions) {
        if (solutions.isAsk()) {
            return Flux.concat(
                    Mono.just(ASK_PROLOGUE),
                    solutions.flux().singleOrEmpty().map(r -> ASK_TRUE).defaultIfEmpty(ASK_FALSE));
        }
        return Flux.concat(Mono.just(createPrologue(solutions.varNames())),
                           encodeRows(solutions),
                           Mono.just(ROWS_EPILOGUE));
    }

    private @NonNull Flux<byte @NonNull []>
    encodeRows(@NonNull QuerySolutions solutions) {
        var names = solutions.varNames();
        var writerTL = ThreadLocal.withInitial(ByteArrayWriter::new);
        AtomicBoolean first = new AtomicBoolean(true);
        return solutions.flux().map(r -> {
            ByteArrayWriter w = writerTL.get().reset();
            if (!first.compareAndExchange(true, false))
                w.append(',');
            try {
                JSONCodec.writeRow(names, r.terms(), w::append);
            } catch (IOException e) {
                throw new RuntimeException("Unexpected IOException", e);
            }
            return w.toByteArray();
        });
    }

    private byte @NonNull [] createPrologue(@NonNull List<String> names) {
        if (names.isEmpty())
            throw new IllegalArgumentException("empty var names list");
        var w = new ByteArrayWriter(44 + 8 * names.size()).append("{\"head\":{\"vars\":[");
        for (int i = 0, size = names.size(); i < size; i++)
            (i == 0 ? w.append("\"") : w.append(",\"")).append(names.get(i)).append("\"");
        return w.append("]},\"results\":{\"bindings\":[").toByteArray();
    }
}
