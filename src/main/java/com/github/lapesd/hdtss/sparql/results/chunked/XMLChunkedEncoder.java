package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import com.github.lapesd.hdtss.sparql.results.codecs.XMLEncoder;
import com.github.lapesd.hdtss.utils.ByteArrayWriter;
import io.micronaut.http.MediaType;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

@Singleton
public class XMLChunkedEncoder implements ChunkedEncoder {
    private static final @NonNull List<@NonNull MediaType> mediaTypes
            = List.of(SparqlMediaTypes.RESULTS_XML_TYPE);
    private static final String BEGIN_HEAD =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">" +
            "<head>";
    private static final byte @NonNull [] BEGIN_ASK =
            (BEGIN_HEAD + "</head><boolean>").getBytes(UTF_8);
    private static final byte @NonNull [] ASK_TRUE  =  "true</boolean></sparql>".getBytes(UTF_8);
    private static final byte @NonNull [] ASK_FALSE = "false</boolean></sparql>".getBytes(UTF_8);
    private static final byte @NonNull [] ROWS_END = "</results></sparql>".getBytes(UTF_8);


    @Override public @NonNull List<@NonNull MediaType> mediaTypes() {
        return mediaTypes;
    }

    @Override
    public @NonNull Flux<byte[]> encode(@NonNull MediaType mediaType,
                                        @NonNull QuerySolutions solutions) {
        if (solutions.isAsk()) {
            return Flux.concat(
                    Mono.just(BEGIN_ASK),
                    solutions.flux().singleOrEmpty().map(r        -> ASK_TRUE)
                                                    .defaultIfEmpty(ASK_FALSE));
        }
        return Flux.concat(
                Mono.just(createPrologue(solutions.varNames())),
                mapSolutions(solutions),
                Mono.just(ROWS_END)
        );
    }

    private @NonNull Flux<byte @NonNull []>
    mapSolutions(@NonNull QuerySolutions solutions) {
        List<@NonNull String> varNames = solutions.varNames();
        var writerTL = ThreadLocal.withInitial(ByteArrayWriter::new);
        return solutions.flux().map(r -> {
            ByteArrayWriter w = writerTL.get().reset();
            try {
                XMLEncoder.writeRow(varNames, r.terms(), w::append);
            } catch (IOException e) {
                throw new RuntimeException("Unexpected IOException", e);
            }
            return w.toByteArray();
        });
    }

    private byte @NonNull [] createPrologue(@NonNull List<@NonNull String> names) {
        if (names.isEmpty())
            throw new IllegalArgumentException("Empty vars list");
        var w = new ByteArrayWriter(BEGIN_HEAD.length()+8*names.size()).append(BEGIN_HEAD);
        for (String name : names) w.append("<variable name=\"").append(name).append("\"/>");
        return w.append("</head><results>").toByteArray();
    }
}
