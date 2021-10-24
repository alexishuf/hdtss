package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.utils.ByteArrayWriter;
import io.micronaut.http.MediaType;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;


abstract class AbstractSVChunkedEncoder implements ChunkedEncoder {
    private final @NonNull List<@NonNull MediaType> mediaType;
    private final char sep;
    private final @NonNull String eol, varSymbol;

    public AbstractSVChunkedEncoder(@NonNull MediaType mediaType,
                                    char sep, @NonNull String eol, @NonNull String varSymbol) {
        this.mediaType = List.of(mediaType);
        this.sep = sep;
        this.eol = eol;
        this.varSymbol = varSymbol;
    }

    @Override public @NonNull List<@NonNull MediaType> mediaTypes() {
        return mediaType;
    }

    @Override
    public @NonNull Flux<byte[]> encode(@NonNull MediaType mediaType,
                                        @NonNull QuerySolutions solutions) {
        Charset charset = mediaType.getCharset().orElse(UTF_8);
        byte[] header = buildHeader(solutions).getBytes(charset);
        var writerTL = ThreadLocal.withInitial(() -> new ByteArrayWriter(charset));
        var rowFlux = solutions.flux().map(r -> {
            ByteArrayWriter writer = writerTL.get().reset();
            Term[] terms = r.terms();
            for (int i = 0; i < terms.length; i++)
                writeTerm(terms[i], i == 0 ? writer : writer.append(sep));
            return writer.append(eol).toByteArray();
        });
        return Flux.concat(Mono.just(header), rowFlux);
    }

    protected abstract void writeTerm(@Nullable Term term, @NonNull ByteArrayWriter writer);

    @NonNull private String buildHeader(@NonNull QuerySolutions solutions) {
        StringBuilder builder = new StringBuilder(solutions.varNames().size()*8);
        for (String name : solutions.varNames())
            builder.append(varSymbol).append(name).append(sep);
        builder.setLength(Math.max(0, builder.length()-1));
        return builder.append(eol).toString();
    }
}
