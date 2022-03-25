package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.codecs.TSVCodec;
import com.github.lapesd.hdtss.utils.ByteArrayWriter;
import io.micronaut.http.MediaType;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

import static com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes.RESULTS_BAD_TSV_TYPE;
import static com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes.RESULTS_TSV_TYPE;

@Singleton
public class TSVChunkedEncoder implements ChunkedEncoder {
    private static final @NonNull List<@NonNull MediaType> MEDIA_TYPES
            = List.of(RESULTS_TSV_TYPE, RESULTS_BAD_TSV_TYPE);

    @Override public @NonNull List<@NonNull MediaType> mediaTypes() {
        return MEDIA_TYPES;
    }

    @Override
    public @NonNull ChunkedPublisher encode(@NonNull MediaType mediaType,
                                            @NonNull QuerySolutions solutions) {
        return new TSVChunkedPublisher(mediaType, solutions);
    }

    private static final class TSVChunkedPublisher extends AbstractSVChunkedPublisher {
        public TSVChunkedPublisher(@NonNull MediaType mt, @NonNull QuerySolutions solutions) {
            super(mt, solutions, "?", "\t", "\n");
        }

        @Override protected void writeTerm(ByteArrayWriter writer, Term term) {
            writer.append(TSVCodec.sanitize(term));
        }
    }
}
